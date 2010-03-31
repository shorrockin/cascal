package com.shorrockin.cascal

import me.prettyprint.cassandra.testutils.EmbeddedServerHelper
import org.junit.{Assert, Test}

class TestEmbeddedCassandra {
  import Assert._
  import Conversions._

   var embedded = new EmbeddedServerHelper()
   lazy val session = new Session("localhost", 9160, Consistency.One)

   def setup() {
    System.out.println("setting up")
    embedded.setup
  }

  def teardown {
    embedded.teardown
    session.close
  }

  @Test def testCanUseSession = {
    setup()
    val basicPath = "NOS" \ "Presence" \ "99" \ ("Hello", "World")

    session.insert(basicPath)
    val contents = session.list(basicPath.owner)
    assertEquals(1, contents.size)
    teardown
  }
}