package com.shorrockin.cascal

import org.junit.{Assert, Test}

/**
 * tests the UUID ability to convert to and from bytes, strings, etc.
 */
class TestUUIDByteConversion {
  import Assert._

  @Test def ensureUUIDConvertsToFromBytes = {
    val original = UUID()
    assertEquals(original, UUID(Conversions.bytes(original)))
  }

  @Test def ensureUUIDConvertsToStringAndBack = {
    val original = UUID()
    val string   = Conversions.string(original)
    assertEquals(original, Conversions.uuid(string))
  }
}