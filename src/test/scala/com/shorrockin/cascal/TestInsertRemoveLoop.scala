package com.shorrockin.cascal

import testing._
import org.junit.{Assert, Test}
import com.shorrockin.cascal.utils.Utils

/**
 * tests a looping insert remove.  Stresses out the precision of
 * system time.
 */
class TestInsertRemoveLoop extends CassandraTestPool {
  import com.shorrockin.cascal.utils.Conversions._
  import Assert._

  @Test def testInsertRemoveLoop = borrow { session =>
    // Uncomment the following like to see if your system time provides enough precision
    // to make this test pass without the workaround.
    // Utils.COMPENSATE_FOR_LOW_PRECISION_SYSTEM_TIME = false
    for( i <- 1L to 100L ) {
      session.remove("Test" \ "Standard" \ "Test")
      session.insert("Test" \ "Standard" \ "Test" \ (i, "hello:"+i))
      assertTrue ( "Could not get inserted value, iteration: "+i, session.get("Test" \ "Standard" \ "Test" \ i) != None )
    }
  }

}