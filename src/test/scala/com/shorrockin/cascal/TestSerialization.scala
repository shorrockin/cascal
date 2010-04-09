package com.shorrockin.cascal

import java.util.Date
import serialization.annotations._
import serialization.{LongSerializer, DateSerializer, Converter}
import utils.Conversions
import org.junit.{Assert, Test}

@Keyspace("Test") @Family("Standard")
case class MappedStandard(@Key val a:String, @Value("Column-B") val b:Date, @Value("Column-C") val c:Long)

@Keyspace("Test") @Family("Super") @Super
case class MappedSuper(@Key val a:String, @SuperColumn val s:String, @Value("Column-B") val b:Date, @Value("Column-C") val c:Long)

@Keyspace("Test") @Family("Standard")
case class MappedOptionStandard(@Optional { val column="Column", val as=classOf[Long] } val value:Option[Long])

@Keyspace("Test") @Family("Super") @Super
case class MappedOptionSuper(@Optional { val column="C", val as=classOf[String] } val value:Option[String])

class TestSerialization {
  import Conversions._
  import Assert._

  @Test def testCanConvertFromColumnsToMappedStandard() {
    val now  = new Date
    val key  = "Test" \ "Standard" \ "Hello"
    val colb = key \ "Column-B" \ now
    val colc = key \ "Column-C" \ 12L
    val cols = colc :: colb

    val obj = Converter[MappedStandard](cols)
    assertEquals("Hello", obj.a)
    assertEquals(now, obj.b)
    assertEquals(12L, obj.c)
  }

  @Test def testCanConvertOptionColumnsToMappedStandard() {
    val key     = "Test" \ "Standard" \ "Hello"
    val valid   = key \ "Column" \ 12
    val inValid = key \ "XYZ" \ 12

    val someObj = Converter[MappedOptionStandard](valid :: Nil)
    val noneObj = Converter[MappedOptionStandard](inValid :: Nil)

    assertTrue(someObj.value.isDefined)
    assertTrue(noneObj.value.isEmpty)
    assertEquals(12L, someObj.value.get)
  }


  @Test def testCanConvertFromMapToSeqObjects() {
    val key = "Test" \\ "Super" \ "Key"
    val col1 = (key \ "SC1" \ "C" \ "Foo") :: Nil
    val col2 = (key \ "SC2" \ "C" \ "Bar") :: Nil
    val map = Map((col1(0).owner -> col1), (col2(0).owner -> col2))

    val objects = Converter[MappedOptionSuper](map)

    assertEquals(2, objects.length)
    assertEquals("Foo", objects(0).value.get)
    assertEquals("Bar", objects(1).value.get)
  }

  @Test def testCanConvertFromColumnsToMappedSuper() {
    val now  = new Date
    val key  = "Test" \\ "Super" \ "Hello"
    val sc   = key \ "Super Column Value"
    val colb = sc \ "Column-B" \ now
    val colc = sc \ "Column-C" \ 12L

    val obj = Converter[MappedSuper](colc :: colb)
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