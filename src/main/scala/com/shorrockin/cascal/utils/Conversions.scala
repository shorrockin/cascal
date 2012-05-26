package com.shorrockin.cascal.utils

import java.nio.charset.Charset
import com.shorrockin.cascal.model.{Column, Keyspace}
import java.util.{Date, UUID => JavaUUID}
import com.shorrockin.cascal.serialization._
import java.nio.ByteBuffer

/**
 * some implicits to assist with common conversions
 */
object Conversions {
  val utf8 = Charset.forName("UTF-8")

  implicit def keyspace(str:String) = new Keyspace(str)

  implicit def bytes(date:Date):ByteBuffer = DateSerializer.toBytes(date)
  implicit def date(bytes:ByteBuffer):Date = DateSerializer.fromBytes(bytes)
  implicit def string(date:Date):String = DateSerializer.toString(date)

  implicit def bytes(b:Boolean):ByteBuffer = BooleanSerializer.toBytes(b)
  implicit def boolean(bytes:ByteBuffer):Boolean = BooleanSerializer.fromBytes(bytes)
  implicit def string(b:Boolean):String = BooleanSerializer.toString(b)

  implicit def bytes(b:Float):ByteBuffer = FloatSerializer.toBytes(b)
  implicit def float(bytes:ByteBuffer):Float = FloatSerializer.fromBytes(bytes)
  implicit def string(b:Float):String = FloatSerializer.toString(b)

  implicit def bytes(b:Double):ByteBuffer = DoubleSerializer.toBytes(b)
  implicit def double(bytes:ByteBuffer):Double = DoubleSerializer.fromBytes(bytes)
  implicit def string(b:Double):String = DoubleSerializer.toString(b)

  implicit def bytes(l:Long):ByteBuffer = LongSerializer.toBytes(l)
  implicit def long(bytes:ByteBuffer):Long = LongSerializer.fromBytes(bytes)
  implicit def string(l:Long):String = LongSerializer.toString(l)

  implicit def bytes(i:Int):ByteBuffer = IntSerializer.toBytes(i)
  implicit def int(bytes:ByteBuffer):Int = IntSerializer.fromBytes(bytes)
  implicit def string(i:Int) = IntSerializer.toString(i)

  implicit def bytes(str:String):ByteBuffer = StringSerializer.toBytes(str)
  implicit def string(bytes:ByteBuffer):String = StringSerializer.fromBytes(bytes)

  implicit def string(source:JavaUUID) = UUIDSerializer.toString(source)
  implicit def uuid(source:String) = UUIDSerializer.fromString(source)
  implicit def bytes(source:JavaUUID):ByteBuffer = UUIDSerializer.toBytes(source)

  implicit def string(col:Column[_]):String = {
    "%s -> %s (time: %s)".format(Conversions.string(col.name),
                                 Conversions.string(col.value),
                                 col.time)
  }

  implicit def toSeqBytes(values:Seq[String]) = values.map { (s) => Conversions.bytes(s) }
  implicit def toJavaList[T](l: Seq[T]):java.util.List[T] = l.foldLeft(new java.util.ArrayList[T](l.size)){(al, e) => al.add(e); al}
}
