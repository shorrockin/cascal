package com.shorrockin.cascal.utils

import java.nio.charset.Charset
import java.util.{UUID => JavaUUID}
import com.shorrockin.cascal.model.{Column, Keyspace}

/**
 * some implicits to assist with common conversions
 */
object Conversions {
  val utf8 = Charset.forName("UTF-8")

  implicit def bytes(str:String) = str.getBytes(utf8)

  implicit def string(bytes:Array[Byte]) = new String(bytes, utf8)

  implicit def keyspace(str:String) = new Keyspace(str)

  implicit def string(source:JavaUUID) = source.toString

  implicit def uuid(source:String) = JavaUUID.fromString(source);

  implicit def bytes(source:JavaUUID):Array[Byte] = {
    val msb = source.getMostSignificantBits()
    val lsb = source.getLeastSignificantBits()
    val buffer = new Array[Byte](16)

    (0 until 8).foreach  { (i) => buffer(i) = (msb >>> 8 * (7 - i)).asInstanceOf[Byte] }
    (8 until 16).foreach { (i) => buffer(i) = (lsb >>> 8 * (7 - i)).asInstanceOf[Byte] }

    buffer
  }

  implicit def string(col:Column[_]):String = {
    "%s -> %s (time: %s)".format(Conversions.string(col.name),
                                 Conversions.string(col.value),
                                 col.time)
  }

  implicit def toSeqBytes(values:Seq[String]) = values.map { (s) => Conversions.bytes(s) }

  implicit def toJavaList[T](l: Seq[T]):java.util.List[T] = l.foldLeft(new java.util.ArrayList[T](l.size)){(al, e) => al.add(e); al}
}