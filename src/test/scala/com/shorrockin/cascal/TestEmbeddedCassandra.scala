package com.shorrockin.cascal

import testing._
import org.junit.{Assert, Test}
import utils.Conversions

class TestEmbeddedCassandra extends CassandraTestPool {
  import Assert._
  import Conversions._

  @Test def testCanUseSession = borrow { (s) =>
    import s._
    val key    = "Test" \ "Standard" \ "testCanUseSessions"
    val column = key \ "Hello" \ "World"

    insert(column)
    assertEquals(1, list(key).size)
  }
}
