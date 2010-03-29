package com.shorrockin.cascal

import model.Keyspace
import java.nio.charset.Charset

/**
 * some implicits to assist with common conversions
 */
object Conversions {
  val utf8 = Charset.forName("UTF-8")

  implicit def stringToKeyspace(str:String) = new Keyspace(str)
  implicit def bytes(str:String) = str.getBytes(utf8)
  implicit def string(bytes:Array[Byte]) = new String(bytes, utf8)
}