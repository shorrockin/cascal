package com.shorrockin.cascal.serialization

import java.nio.ByteBuffer
import reflect.Manifest
import java.lang.annotation.Annotation
import java.lang.reflect.{Field, Method}
import java.util.{Date, UUID}
import annotations.{Columns, Optional}
import annotations.{Key => AKey, SuperColumn => ASuperColumn, Value => AValue}
import annotations.{Keyspace => AKeySpace, Super => ASuper, Family => AFamily}
import com.shorrockin.cascal.model._
import com.shorrockin.cascal.utils.{Logging, Conversions}

/**
 * holds a reference to the default converter
 */
object Converter extends Converter(Serializer.Default) with Logging {
}

/**
 * main class used to convert objects to and from their cascal
 * equivalents.
 *
 * @author Chris Shorrock
 */
class Converter(serializers:Map[Class[_], Serializer[_]]) {

  private var reflectionCache = Map[Class[_], ReflectionInformation]()


  /**
   * converts all the column sequences in the provided map (which is returned from a list
   * call). and returns a sequence of the specified type.
   */
  def apply[T](seq:Seq[(SuperColumn, Seq[Column[SuperColumn]])])(implicit manifest:Manifest[T]):Seq[T] = {
    seq.map { (tup) => apply[T](tup._2) }
  }


  /**
   * given a list of columns, assumed to all belong to the same columns, creates
   * the object of type T using the annotations present an that class. Uses
   * the serializers to convert values in columns to their appropriate.
   */
  def apply[T](columns:Seq[Column[_]])(implicit manifest:Manifest[T]):T = {
    val info = Converter.this.info(manifest.erasure)

    // iterate over all the types and map them into the values needed to call the
    // constructor to create the object.
    val values:Seq[Any] = info.parameters.map { (paramType) =>
      val cls        = paramType._1
      val annotation = paramType._2

      annotation match {
        // if there's a columnsToKey annotation get the first columnsToKey in the columns and return it
        case k:AKey => stringToObject(cls, columnsToKey(columns).value)

        // if there's a super column annotation get the super column then use the serializers
        // to convert the byte array to the appropriate value.
        case sc:ASuperColumn => info.isSuper match {
          case true  => bytesToObject(cls, columnsToSuperColumn(columns).value)
          case false => throw new IllegalArgumentException("@SuperColumn may only exist within class annotated with @Super")
        }

        // if there's a columns annotation that is mapped to a Seq[(Tuple, Tuple)] then iterate
        // over all the columns returned and create the appropriate type using values provided.
        case a:Columns => cls.equals(classOf[Seq[_]]) match {
          case false => throw new IllegalArgumentException("@Columns annotation must be attached to Seq[Tuple2] values - was: " + cls)
          case true  => columns.map { (column) => (bytesToObject(a.name, column.name) -> bytesToObject(a.value, column.value)) }
        }

        // if there's a value annotation look up the column with the matching name and then
        // retrieve the value, and convert it as needed.
        case a:AValue => find(a.value, columns) match {
          case None    => throw new IllegalArgumentException("Unable to find column with name: " + a.value)
          case Some(c) => bytesToObject(cls, c.value)
        }

        // optional types are like values except they map to option/some/none so they may or
        // may not exist. additionally - we get the parameter type from the annotation not
        // the actual parameter
        case a:Optional => cls.equals(classOf[Option[_]]) match {
          case true => find(a.column, columns) match {
            case None    => None
            case Some(c) => Some(bytesToObject(a.as, c.value))
          }
          case false => throw new IllegalArgumentException("@Optional may only be used on a Option[_] parameter")
        }


        // anything else throw an exception
        case _ => throw new IllegalStateException("annonation of: " + annotation + " was not placed in such a manner that it could be used on type: " + cls)
      }
    }

    info.constructor.newInstance(values.toArray.asInstanceOf[Array[Object]]:_*).asInstanceOf[T]
  }

  /**
   * Given a class type, a Method that returns that type, and a source object (Cascal ORM object),
   * return the appropriate serialized byte array. Does not support Option.
   */
  private def getFieldSerialized[T](fieldType:Class[_], fieldGetter:Method, obj:T):ByteBuffer = {
    // Couldn't figure out how to case match classes on a class obj with type erasure
    if (fieldType == classOf[String]) Conversions.bytes(fieldGetter.invoke(obj).asInstanceOf[String])
    else if (fieldType == classOf[UUID]) Conversions.bytes(fieldGetter.invoke(obj).asInstanceOf[UUID])
    else if (fieldType == classOf[Int]) Conversions.bytes(fieldGetter.invoke(obj).asInstanceOf[Int])
    else if (fieldType == classOf[Long]) Conversions.bytes(fieldGetter.invoke(obj).asInstanceOf[Long])
    else if (fieldType == classOf[Boolean]) Conversions.bytes(fieldGetter.invoke(obj).asInstanceOf[Boolean])
    else if (fieldType == classOf[Float]) Conversions.bytes(fieldGetter.invoke(obj).asInstanceOf[Float])
    else if (fieldType == classOf[Double]) Conversions.bytes(fieldGetter.invoke(obj).asInstanceOf[Double])
    else if (fieldType == classOf[Date]) Conversions.bytes(fieldGetter.invoke(obj).asInstanceOf[Date])
    else throw new IllegalStateException("Type %s of getter %s is unknown".format(fieldGetter.getName, fieldType.toString))
  }

  /**
   * Given a Method that returns an Option, and a source object (Cascal ORM object),
   * return null if calling the method returns None, or otherwise the appropriate
   * serialized byte array.
   */
  private def getOptionFieldSerialized[T](fieldGetter:Method, obj:T):ByteBuffer = {
    val opt = fieldGetter.invoke(obj).asInstanceOf[Option[_]]
    opt match {
      case None => null
      case Some(x:String) => Conversions.bytes(x)
      case Some(x:UUID) => Conversions.bytes(x)
      case Some(x:Int) => Conversions.bytes(x)
      case Some(x:Long) => Conversions.bytes(x)
      case Some(x:Boolean) => Conversions.bytes(x)
      case Some(x:Float) => Conversions.bytes(x)
      case Some(x:Double) => Conversions.bytes(x)
      case Some(x:Date) => Conversions.bytes(x)
      case _ => throw new IllegalStateException(
          "Type of Option %s for getter %s is unknown".format(opt.toString, fieldGetter.getName))
    }
  }

  /**
   * Given an object of type T using the Cascal Annotations returns a list of columns
   * complete with name/value. Uses the serializers to convert values in columns to their
   * appropriate byte array.
   */
  def unapply[T](obj:T)(implicit manifest:Manifest[T]):List[Column[_]] = {
    val info = Converter.this.info(manifest.erasure)

    val key:String = info.fieldGettersAndColumnNames.filter(tup => tup._2._2 match {
      case a:AKey => true
      case _ => false
    }).head._1.invoke(obj).asInstanceOf[String]

    var superCol:ByteBuffer = null
    if (info.isSuper) {
      val superTup = info.fieldGettersAndColumnNames.filter(tup => tup._2._2 match {
        case a:ASuperColumn => true
        case _ => false
      }).head
      val superGetter = superTup._1
      val superType = superTup._2._1
      superCol = getFieldSerialized(superType, superGetter, obj)
    }

    info.fieldGettersAndColumnNames.foldLeft(List[Column[_]]()) { (acc, tup) =>
      val fieldGetter = tup._1
      var optField = false
      val fieldType = tup._2._2 match {
        case a:Optional =>
          optField = true
          a.as
        case _ => tup._2._1
      }
      val columnName:String = tup._2._2 match {
        case a:Optional => a.column
        case a:AValue => a.value
        case _ => null
      }

      val value:ByteBuffer = optField match {
        case false => getFieldSerialized(fieldType, fieldGetter, obj)
        case true => getOptionFieldSerialized(fieldGetter, obj)
      }

      if (columnName == null || value == null) acc
      else info.isSuper match {
        case true => (info.family.asInstanceOf[SuperColumnFamily] \ key \ superCol \ (Conversions.bytes(columnName), value)) :: acc
        case false => (info.family.asInstanceOf[StandardColumnFamily] \ key \ (Conversions.bytes(columnName), value)) :: acc
      }
    }
  }

  /**
   * returns the reflection information from the reflection cache, using
   * DCL to manage access to the cache.
   */
  def info(cls:Class[_]):ReflectionInformation = {
    if (reflectionCache.contains(cls)) reflectionCache(cls)
    else this.synchronized {
      if (reflectionCache.contains(cls)) reflectionCache(cls)
      else {
        val out = ReflectionInformation(cls)
        reflectionCache = reflectionCache + (out.cls.asInstanceOf[Class[_]] -> out)
        out
      }
    }
  }


  /**
   * returns the column with the specified name, or
   */
  private def find(name:String, columns:Seq[Column[_]]):Option[Column[_]] = {
    val nameBytes = Conversions.bytes(name)
    columns.find { (c) => nameBytes.equals(c.name) }
  }


  /**
   * converts the specified byte array to the specified type using the installed
   * serializers.
   */
  private def bytesToObject[A](ofType:Class[A], bytes:ByteBuffer):A = {
    serializers.get(ofType) match {
      case None    => throw new IllegalArgumentException("unable to find serializer for type: " + ofType)
      case Some(s) =>
        // TODO sure there's a better way - without this you end up with:
        //   "value asInstanceOf is not a member of ?"
        val castedSerial = s.asInstanceOf[Serializer[Any]]
        (castedSerial.fromBytes(bytes)).asInstanceOf[A]
    }
  }

  /**
   * converts the specified string to the specified type using the installed
   * serializers.
   */
  private def stringToObject[A](ofType:Class[A], string:String):A = {
    serializers.get(ofType) match {
      case None    => throw new IllegalArgumentException("unable to find serializer for type: " + ofType)
      case Some(s) =>
        // TODO sure there's a better way - without this you end up with:
        //   "value asInstanceOf is not a member of ?"
        val castedSerial = s.asInstanceOf[Serializer[Any]]
        (castedSerial.fromString(string)).asInstanceOf[A]
    }
  }


  /**
   * returns the common super column that is shared amonst all the columns
   */
  private def columnsToSuperColumn(columns:Seq[Column[_]]):SuperColumn = {
    if (columns.length == 0) throw new IllegalArgumentException("unable to retrieve super column when Seq[Column] is empty")
    columns(0).owner match {
      case sc:SuperColumn => sc
      case _ => throw new IllegalArgumentException("unable to retrieve super column for a standard column")
    }
  }


  /**
   * returns the columnsToKey value for the specified sequence of columns, assumes all columns
   * contain the same columnsToKey.
   */
  private def columnsToKey(columns:Seq[Column[_]]):Key[_, _] = {
    if (columns.length == 0) throw new IllegalArgumentException("unable to retrieve key value when empty list of columns are provided")
    columns(0).key
  }


  /**
   * holds reflection information about a given class
   */
  case class ReflectionInformation(val cls:Class[_]) {
    val keyspace = {
      extract(cls, classOf[AKeySpace]) match {
        case None    => throw new IllegalArgumentException("all mapped classes must contain @Keyspace annotation; not found in " + cls)
        case Some(v) => Keyspace(v.value())
      }
    }

    val isSuper = {
      extract(cls, classOf[ASuper]) match {
        case None    => false
        case Some(v) => true
      }
    }

    val family = {
      extract(cls, classOf[AFamily]) match {
        case None    => throw new IllegalArgumentException("all mapped classes must contain @Family annotation")
        case Some(f) => isSuper match {
          case true  => SuperColumnFamily(f.value(), keyspace)
          case false => StandardColumnFamily(f.value(), keyspace)
        }
      }
    }

    // TODO examine all - use one with annotations present
    val constructor = cls.getDeclaredConstructors()(0)

    val parameters = {
      val params      = constructor.getParameterTypes
      val annotations = constructor.getParameterAnnotations
      var out         = List[(Class[_], Annotation)]()

      (0 until params.length).foreach { (index) =>
         val annotation = annotations(index)

         if (null == annotation || 0 == annotation.length) {
           throw new IllegalArgumentException("unable to create object when not all parameters have annotations, parameter type: " + params(index) + ", index: " + index)
         }

         if (1 != annotation.length) {
           throw new IllegalArgumentException("in a cascal mapped object each argument must have ONLY one annotation")
         }

         out = (params(index) -> annotation(0)) :: out
      }

      out.reverse
    }

    // map of annotation classes to the field
    val fields = {
      var out = List[(Field, Annotation)]()
      cls.getDeclaredFields.foreach { field =>
        val annotations = field.getDeclaredAnnotations
        if (annotations.length > 0) annotations(0) match {
          case a:AKey         => out = (field -> a) :: out
          case a:Optional     => out = (field -> a) :: out
          case a:ASuperColumn => out = (field -> a) :: out
          case a:AValue       => out = (field -> a) :: out
          case _ => /* ignore */
        }
      }
      out
    }

    val fieldNames = cls.getDeclaredFields.map(_.getName)
    val fieldGetters = cls.getDeclaredMethods.filter(m=>fieldNames.contains(m.getName))
    // Returns Seq[(getters for private field, (col type, col annotation))]
    val fieldGettersAndColumnNames = fieldGetters.sortWith(
        (f1, f2) => fieldNames.indexOf(f1.getName) < fieldNames.indexOf(f2.getName)).zip(parameters)

    /**
     * returns the field for the specified annotation class
     */
    def field[A <: Annotation](cls:Class[A]):Option[(Field, Annotation)] = fields.find { (tup) => cls.equals(tup._2.getClass)}

    /**
     * returns all the fields matching the specified annotation
     */
    def fields[A <: Annotation](cls:Class[A]):Seq[(Field, Annotation)] = fields.filter { (tup) => cls.equals(tup._2.getClass) }

    private def extract[A <: Annotation](cls:Class[_], annot:Class[A]):Option[A] = {
      val value = cls.getAnnotation(annot).asInstanceOf[A]
      if (null == value) None
      else Some(value)
    }
  }
}
