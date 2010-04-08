package com.shorrockin.cascal.serialization

import com.shorrockin.cascal.utils.Conversions._
import java.util.UUID
import java.util.Date

object Serializer {

  /**
   * defines a map of all the default serializers
   */
  val Default = Map[Class[_], Serializer[_]](
    (classOf[String]  -> StringSerializer),
    (classOf[UUID]    -> UUIDSerializer),
    (classOf[Int]     -> IntSerializer),
    (classOf[Long]    -> LongSerializer),
    (classOf[Boolean] -> BooleanSerializer),
    (classOf[Float]   -> FloatSerializer),
    (classOf[Double]  -> DoubleSerializer),
    (classOf[Date]    -> DateSerializer)
  )

  /**
   * convienence method for creating a serializer
   */
  def apply[T](to:(T) => Array[Byte])(from:(Array[Byte]) => T) = {
    new Serializer[T] {
      def toBytes(obj:T) = to(obj)
      def fromBytes(bytes:Array[Byte]) = from(bytes)
    }
  }
}

/**
 *  defines a class responsible for converting an object to and from an
 * array of bytes.
 *
 * @author Chris Shorrock
 */
trait Serializer[A] {
  /** converts this object to a byte array for entry into cassandra */
  def toBytes(obj:A):Array[Byte]

  /** converts the specified byte array into an object */
  def fromBytes(bytes:Array[Byte]):A
}

object StringSerializer extends Serializer[String] {
  def toBytes(str:String) = bytes(str)
  def fromBytes(bytes:Array[Byte]) = string(bytes)
}

object UUIDSerializer extends Serializer[UUID] {
  def toBytes(uuid:UUID) = bytes(uuid)
  def fromBytes(bytes:Array[Byte]) = uuid(bytes)
}

object IntSerializer extends Serializer[Int] {
  def toBytes(i:Int) = StringSerializer.toBytes(i.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toInt
}

object LongSerializer extends Serializer[Long] {
  def toBytes(l:Long) = StringSerializer.toBytes(l.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toLong
}

object BooleanSerializer extends Serializer[Boolean] {
  def toBytes(b:Boolean) = StringSerializer.toBytes(b.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toBoolean
}

object FloatSerializer extends Serializer[Float] {
  def toBytes(f:Float) = StringSerializer.toBytes(f.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toFloat
}

object DoubleSerializer extends Serializer[Double] {
  def toBytes(d:Double) = StringSerializer.toBytes(d.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toDouble
}

object DateSerializer extends Serializer[Date] {
  def toBytes(date:Date) = LongSerializer.toBytes(date.getTime)
  def fromBytes(bytes:Array[Byte]) = new Date(LongSerializer.fromBytes(bytes).longValue)
}