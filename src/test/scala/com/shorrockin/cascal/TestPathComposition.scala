package com.shorrockin.cascal

import org.junit.{Assert, Test}

/**
 * really simple test to ensure that basic path composition works
 */
class TestPathComposition {
  import com.shorrockin.cascal.Conversions._
  import Assert._

  @Test def ensureCanComposeStandardPath() {
    val out = "Keyspace" \ "StandardFamily" \ "StandardKey" \ "ColumnName" \ "ColumnValue"
    assertEquals("ColumnValue", string(out.value))
    assertEquals("ColumnName", string(out.name.value))
    assertEquals("StandardKey", out.owner.value)
    assertEquals("StandardKey", out.key.value)
    assertEquals("StandardFamily", out.family.value)
    assertEquals("Keyspace", out.keyspace.value)
  }

  @Test def ensureCanComposeSuperPath() {
    val out = "Keyspace" \\ "SuperFamily" \ "SuperKey" \ "SuperColumn" \ "ColumnName" \ "ColumnValue"
    assertEquals("ColumnValue", string(out.value))
    assertEquals("ColumnName", string(out.name.value))
    assertEquals("SuperColumn", string(out.owner.value))
    assertEquals("SuperKey", out.key.value)
    assertEquals("SuperFamily", out.family.value)
    assertEquals("Keyspace", out.keyspace.value)
  }
}