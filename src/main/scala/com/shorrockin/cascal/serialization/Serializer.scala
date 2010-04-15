package com.shorrockin.cascal.serialization

import com.shorrockin.cascal.utils.{UUID => UUIDUtils}
import java.util.UUID
import java.util.Date
import java.nio.charset.Charset

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

  /** converts the specified value to a string */
  def toString(obj:A):String

  /** converts the specified value from a string */
  def fromString(str:String):A
}

object StringSerializer extends Serializer[String] {
  val utf8 = Charset.forName("UTF-8")

  def toBytes(str:String) = str.getBytes(utf8)
  def fromBytes(bytes:Array[Byte]) = new String(bytes, utf8)
  def toString(str:String) = str
  def fromString(str:String) = str
}

object UUIDSerializer extends Serializer[UUID] {
  def fromBytes(bytes:Array[Byte]) = UUIDUtils(bytes)
  def toString(uuid:UUID) = uuid.toString
  def fromString(str:String) = UUID.fromString(str)

  def toBytes(uuid:UUID) = {
    val msb = uuid.getMostSignificantBits()
    val lsb = uuid.getLeastSignificantBits()
    val buffer = new Array[Byte](16)

    (0 until 8).foreach  { (i) => buffer(i) = (msb >>> 8 * (7 - i)).asInstanceOf[Byte] }
    (8 until 16).foreach { (i) => buffer(i) = (lsb >>> 8 * (7 - i)).asInstanceOf[Byte] }

    buffer
  }

}

object IntSerializer extends Serializer[Int] {
  def toBytes(i:Int) = StringSerializer.toBytes(i.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toInt
  def toString(obj:Int) = obj.toString
  def fromString(str:String) = str.toInt
}

object LongSerializer extends Serializer[Long] {
  def toBytes(l:Long) = StringSerializer.toBytes(l.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toLong
  def toString(obj:Long) = obj.toString
  def fromString(str:String) = str.toLong
}

object BooleanSerializer extends Serializer[Boolean] {
  def toBytes(b:Boolean) = StringSerializer.toBytes(b.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toBoolean
  def toString(obj:Boolean) = obj.toString
  def fromString(str:String) = str.toBoolean
}

object FloatSerializer extends Serializer[Float] {
  def toBytes(f:Float) = StringSerializer.toBytes(f.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toFloat
  def toString(obj:Float) = obj.toString
  def fromString(str:String) = str.toFloat
}

object DoubleSerializer extends Serializer[Double] {
  def toBytes(d:Double) = StringSerializer.toBytes(d.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toDouble
  def toString(obj:Double) = obj.toString
  def fromString(str:String) = str.toDouble
}

object DateSerializer extends Serializer[Date] {
  def toBytes(date:Date) = LongSerializer.toBytes(date.getTime)
  def fromBytes(bytes:Array[Byte]) = new Date(LongSerializer.fromBytes(bytes).longValue)
  def toString(obj:Date) = obj.getTime.toString
  def fromString(str:String) = new Date(str.toLong.longValue)
}