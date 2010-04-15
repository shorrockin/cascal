package com.shorrockin.cascal.utils

import java.nio.charset.Charset
import com.shorrockin.cascal.model.{Column, Keyspace}
import java.util.{Date, UUID => JavaUUID}
import com.shorrockin.cascal.serialization._

/**
 * some implicits to assist with common conversions
 */
object Conversions {
  val utf8 = Charset.forName("UTF-8")

  implicit def keyspace(str:String) = new Keyspace(str)

  implicit def bytes(date:Date):Array[Byte] = DateSerializer.toBytes(date)
  implicit def date(bytes:Array[Byte]):Date = DateSerializer.fromBytes(bytes)
  implicit def string(date:Date):String = DateSerializer.toString(date)

  implicit def bytes(b:Boolean):Array[Byte] = BooleanSerializer.toBytes(b)
  implicit def boolean(bytes:Array[Byte]):Boolean = BooleanSerializer.fromBytes(bytes)
  implicit def string(b:Boolean):String = BooleanSerializer.toString(b)

  implicit def bytes(b:Float):Array[Byte] = FloatSerializer.toBytes(b)
  implicit def float(bytes:Array[Byte]):Float = FloatSerializer.fromBytes(bytes)
  implicit def string(b:Float):String = FloatSerializer.toString(b)

  implicit def bytes(b:Double):Array[Byte] = DoubleSerializer.toBytes(b)
  implicit def double(bytes:Array[Byte]):Double = DoubleSerializer.fromBytes(bytes)
  implicit def string(b:Double):String = DoubleSerializer.toString(b)

  implicit def bytes(l:Long):Array[Byte] = LongSerializer.toBytes(l)
  implicit def long(bytes:Array[Byte]):Long = LongSerializer.fromBytes(bytes)
  implicit def string(l:Long):String = LongSerializer.toString(l)

  implicit def bytes(i:Int):Array[Byte] = IntSerializer.toBytes(i)
  implicit def int(bytes:Array[Byte]):Int = IntSerializer.fromBytes(bytes)
  implicit def string(i:Int) = IntSerializer.toString(i)

  implicit def bytes(str:String):Array[Byte] = StringSerializer.toBytes(str)
  implicit def string(bytes:Array[Byte]):String = StringSerializer.fromBytes(bytes)

  implicit def string(source:JavaUUID) = UUIDSerializer.toString(source)
  implicit def uuid(source:String) = UUIDSerializer.fromString(source)
  implicit def bytes(source:JavaUUID):Array[Byte] = UUIDSerializer.toBytes(source)

  implicit def string(col:Column[_]):String = {
    "%s -> %s (time: %s)".format(Conversions.string(col.name),
                                 Conversions.string(col.value),
                                 col.time)
  }

  implicit def toSeqBytes(values:Seq[String]) = values.map { (s) => Conversions.bytes(s) }
  implicit def toJavaList[T](l: Seq[T]):java.util.List[T] = l.foldLeft(new java.util.ArrayList[T](l.size)){(al, e) => al.add(e); al}
}