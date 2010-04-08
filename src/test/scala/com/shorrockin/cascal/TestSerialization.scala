package com.shorrockin.cascal

import java.util.Date
import serialization.annotations._
import serialization.{LongSerializer, DateSerializer, Converter}
import utils.Conversions
import org.junit.{Assert, Test}

@Keyspace("Test") @Family("Standard")
case class MappedStandard(@Key val a:String, @Value("Column-B") val b:Date, @Value("Column-C") val c:Long)

@Keyspace("Test") @Super @Family("Super")
case class MappedSuper(@Key val a:String, @SuperColumn val s:String, @Value("Column-B") val b:Date, @Value("Column-C") val c:Long)

@Keyspace("Test") @Family("Standard")
case class MappedOptionStandard(@Optional { val column="Column", val as=classOf[Long] } val value:Option[Long])

class TestSerialization {
  import Conversions._
  import Assert._

  @Test def testCanConvertFromColumnsToMappedStandard() {
    val now  = new Date
    val key  = "Test" \ "Standard" \ "Hello"
    val colb = key \ "Column-B" \ now
    val colc = key \ "Column-C" \ 12L
    val cols = colc :: colb

    val obj = Converter.toObject[MappedStandard](cols)
    assertEquals("Hello", obj.a)
    assertEquals(now, obj.b)
    assertEquals(12L, obj.c)
  }

  @Test def testCanConvertOptionColumnsToMappedStandard() {
    val key     = "Test" \ "Standard" \ "Hello"
    val valid   = key \ "Column" \ 12
    val inValid = key \ "XYZ" \ 12

    val someObj = Converter.toObject[MappedOptionStandard](valid :: Nil)
    val noneObj = Converter.toObject[MappedOptionStandard](inValid :: Nil)

    assertTrue(someObj.value.isDefined)
    assertTrue(noneObj.value.isEmpty)
    assertEquals(12L, someObj.value.get)
  }

  @Test def testCanConvertFromColumnsToMappedSuper() {
    val now  = new Date
    val key  = "Test" \\ "Super" \ "Hello"
    val sc   = key \ "Super Column Value"
    val colb = sc \ "Column-B" \ now
    val colc = sc \ "Column-C" \ 12L

    val obj = Converter.toObject[MappedSuper](colc :: colb)
    assertEquals("Hello", obj.a)
    assertEquals("Super Column Value", obj.s)
    assertEquals(now, obj.b)
    assertEquals(12L, obj.c)    
  }


  @Test def testCanConvertObjectToStandardColumnList() {

  }


  @Test def testCanConvertObjectToSuperColumnList() {

  }
}