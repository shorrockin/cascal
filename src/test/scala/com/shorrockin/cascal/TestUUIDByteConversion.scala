package com.shorrockin.cascal

import serialization.annotations._
import serialization.Converter
import testing._
import org.junit.{Assert, Test}
import utils.{UUID, Conversions}
import java.util.{UUID => JavaUUID, Date}

/**
 * tests the UUID ability to convert to and from bytes, strings, etc.
 */
class TestUUIDByteConversion extends CassandraTestPool {
  import Assert._
  import Conversions._

  @Test def ensureUUIDConvertsToFromBytes = {
    val original = UUID()
    assertEquals(original, UUID(Conversions.bytes(original)))
  }

  @Test def ensureUUIDConvertsToStringAndBack = {
    val original = UUID()
    val string   = Conversions.string(original)
    assertEquals(original, Conversions.uuid(string))
  }

  @Test def testUUIDColumnMapping {
    val uuid     = UUID()
    val toInsert = "Test" \\ "Super" \ "testUUIDColumnMapping" \ uuid \ "Column" \ "Value"
    borrow { session =>
      session.insert(toInsert)
      val result = Converter[MappedUUID](session.list("Test" \\ "Super" \ "testUUIDColumnMapping"))

      assertEquals(uuid, result(0).s)
      assertEquals("Value", result(0).b)
    }
  }
}

@Keyspace("Test") @Family("Super") @Super
case class MappedUUID(@Key val a:String, @SuperColumn val s:JavaUUID, @Value("Column") val b:String)
