package com.shorrockin.cascal

import model.Keyspace
import java.nio.charset.Charset
import java.util.{UUID => JavaUUID}

/**
 * some implicits to assist with common conversions
 */
object Conversions {
  val utf8 = Charset.forName("UTF-8")
  val emptyBytes = new Array[Byte](0)

  implicit def bytes(str:String) = str.getBytes(utf8)

  implicit def string(bytes:Array[Byte]) = new String(bytes, utf8)

  implicit def keyspace(str:String) = new Keyspace(str)

  implicit def string(source:JavaUUID) = new String(bytes(source), Conversions.utf8)

  implicit def bytes(source:JavaUUID):Array[Byte] = {
    val msb = source.getMostSignificantBits()
    val lsb = source.getLeastSignificantBits()
    val buffer = new Array[Byte](16)

    (0 until 8).foreach  { (i) => buffer(i) = (msb >>> 8 * (7 - i)).asInstanceOf[Byte] }
    (8 until 16).foreach { (i) => buffer(i) = (lsb >>> 8 * (7 - i)).asInstanceOf[Byte] }

    buffer
  }
}