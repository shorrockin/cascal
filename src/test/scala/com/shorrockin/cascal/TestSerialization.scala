package com.shorrockin.cascal

import java.util.Date
import serialization.annotations._
import serialization.{LongSerializer, DateSerializer, Converter}
import utils.Conversions
import org.junit.{Assert, Test}

@Keyspace("Test") @Family("Standard")
case class MappedStandard(@Key val a:String,
                          @Value("Column-B") val b:Date,
                          @Value("Column-C") val c:Long)

@Keyspace("Test") @Super @Family("Super")
case class MappedSuper(@Key val a:String,
                       @SuperColumn val s:String,
                       @Value("Column-B") val b:Date,
                       @Value("Column-C") val c:Long)

class TestSerialization {
  import Conversions._
  import Assert._

  @Test def testCanConvertFromColumnsToMappedStandard() {

    val key  = "Test" \ "Standard" \ "Hello"
    val date = new Date
    val colb = key \ ("Column-B", DateSerializer.toBytes(date))
    val colc = key \ ("Column-C", LongSerializer.toBytes(12L))
    val cols = colc :: colb

    val obj = Converter.toObject[MappedStandard](cols)
    assertEquals("Hello", obj.a)
    assertEquals(date, obj.b)
    assertEquals(12L, obj.c)
  }

  @Test def testCanConvertFromColumnsToMappedSuper() {
  }
}