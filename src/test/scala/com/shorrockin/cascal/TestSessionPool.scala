package com.shorrockin.cascal

import org.junit.Test


class TestSessionPool {
  import Conversions._

  @Test def testSessionPool = {
    EmbeddedTestCassandra.init

    val hosts  = Host("localhost", 9160, 250) :: Host("localhost", 9161, 1)
    val params = new PoolParams(10, ExhaustionPolicy.Fail, 500L, 6, 2)
    val pool   = new SessionPool(hosts, params, Consistency.One)

    // as long as no exceptions were thrown we passed
    (0 until 10).foreach { index =>
      pool.borrow { _.count("Test" \ "Standard" \ UUID()) }
    }


  }
}