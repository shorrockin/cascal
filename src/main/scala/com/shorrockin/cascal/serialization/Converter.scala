package com.shorrockin.cascal.serialization

import reflect.Manifest
import annotations.{Keyspace => AKeySpace, Family => AFamily, Super => ASuper, Key => AKey, Value => AValue, SuperColumn => ASuperColumn, Optional}
import com.shorrockin.cascal.model._
import java.lang.annotation.Annotation
import java.lang.reflect.Constructor
import java.util.Arrays
import com.shorrockin.cascal.utils.{Logging, Conversions}

/**
 * holds a reference to the default converter
 */
object Converter extends Converter(Serializer.Default) with Logging {
}

/**
 *  main class used to convert objects to and from their cascal
 * equivilants.
 *
 * @author Chris Shorrock
 */
class Converter(serializers:Map[Class[_], Serializer[_]]) {
  import Converter.log

  /**
   * given a list of columns, assumed to all belong to the same key, creates
   * the object of type T using the annotations present an that class. Uses
   * the serializers to convert values in columns to their appropriate.
   */
  def toObject[T](columns:Seq[Column[_]])(implicit manifest:Manifest[T]):T = {
    val cls        = manifest.erasure
    val ks         = keyspace(cls)
    val superCf    = isSuper(cls)
    val fam        = family(cls, superCf, ks)
    val const      = cls.getDeclaredConstructors()(0) // TODO examine all - use one with annotations present
    val paramTypes = constructorParams(const)

    // iterate over all the types and map them into the values needed to call the
    // constructor to create the object.
    val paramValues:Seq[Any] = paramTypes.map { (pType) =>
      val paramClass  = pType._1
      val paramAnnot  = pType._2
      paramAnnot match {
        // if there's a key annotation get the first key in the columns and return it
        case k:AKey if (paramClass.equals(classOf[String])) => key(columns).value

        // if there's a super column annotation get the super column then use the serializers
        // to convert the byte array to the appropriate value.
        case sc:ASuperColumn if (superCf) => toObject(paramClass, superColumn(columns).value)

        // if there's a value annotation look up the column with the matching name and then
        // retrieve the value, and convert it as needed.
        case a:AValue => columnNamed(a.value, columns) match {
          case None    => throw new IllegalArgumentException("unable to find column with name: " + a.value)
          case Some(c) => toObject(paramClass, c.value)
        }

        // optional types are like values except they map to option/some/none so they may or
        // may not exist. additionally - we get the parameter type from the annotation not
        // the actual parameter
        case a:Optional if (paramClass.equals(classOf[Option[_]])) => columnNamed(a.column, columns) match {
          case None    => None
          case Some(c) => Some(toObject(a.as, c.value))
        }

        // anything else throw an exception
        case _ => throw new IllegalStateException("annonation of: " + paramAnnot + " was not placed in such a manner that it could be used")
      }
    }

    const.newInstance(paramValues.toArray.asInstanceOf[Array[Object]]:_*).asInstanceOf[T]
  }





  /**
   * returns the column with the specified name, or 
   */
  private def columnNamed(name:String, columns:Seq[Column[_]]):Option[Column[_]] = {
    val nameBytes = Conversions.bytes(name)
    columns.find { (c) => Arrays.equals(nameBytes, c.name) }
  }


  /**
   * converts the specified byte array to the specified type using the installed
   * serializers.
   */
  private def toObject[A](ofType:Class[A], bytes:Array[Byte]):A = {
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
   * returns the common super column that is shared amonst all the columns
   */
  private def superColumn(columns:Seq[Column[_]]):SuperColumn = {
    if (columns.length == 0) throw new IllegalArgumentException("unable to retrieve super column when Seq[Column] is empty")
    columns(0).owner match {
      case sc:SuperColumn => sc
      case _ => throw new IllegalArgumentException("unable to retrieve super column for a standard column")
    }
  }


  /**
   * returns the key value for the specified sequence of columns, assumes all columns
   * contain the same key.
   */
  private def key(columns:Seq[Column[_]]):Key[_, _] = {
    if (columns.length == 0) throw new IllegalArgumentException("unable to retrieve key value when Seq[Column] is empty")
    columns(0).key
  }


  /**
   * given a constructor composes a sequence of class types to the annotations that are present
   * on that parameters
   */
  private def constructorParams(cls:Constructor[_]):Seq[(Class[_], Annotation)] = {
    val params      = cls.getParameterTypes
    val annotations = cls.getParameterAnnotations
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

  /**
   * extracts the column family, throwing an exception in the the @Family annotation
   * is not present.
   */
  private def family(cls:Class[_], isSuper:Boolean, ks:Keyspace):ColumnFamily[_] = {
    extract(cls, classOf[AFamily]) match {
      case None    => throw new IllegalArgumentException("all mapped classes must contain @Family annotation")
      case Some(f) => isSuper match {
        case true  => SuperColumnFamily(f.value(), ks)
        case false => StandardColumnFamily(f.value(), ks)
      }
    }
  }


  /**
   * extracts the keyspace from the class throwing an exception if the @Keyspace
   * annotation was not provided.
   */
  private def isSuper(cls:Class[_]):Boolean = {
    extract(cls, classOf[ASuper]) match {
      case None    => false
      case Some(v) => true
    }
  }


  /**
   * extracts the keyspace from the class throwing an exception if the @Keyspace
   * annotation was not provided.
   */
  private def keyspace(cls:Class[_]):Keyspace = {
    extract(cls, classOf[AKeySpace]) match {
      case None    => throw new IllegalArgumentException("all mapped classes must contain @Keyspace annotation")
      case Some(v) => Keyspace(v.value())
    }
  }


  /**
   * extracts the specified class annotation from the class
   */
  private def extract[A <: Annotation](cls:Class[_], annot:Class[A]):Option[A] = {
    val value = cls.getAnnotation(annot).asInstanceOf[A]
    if (null == value) None
    else Some(value)
  }
}