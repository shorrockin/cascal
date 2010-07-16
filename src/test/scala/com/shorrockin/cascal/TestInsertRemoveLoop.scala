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

    def checkLowResolution = {
      var onLowPrecisionSystem = false
      for( i <- 1L to 100L ) {
        session.remove("Test" \ "Standard" \ "Test")
        session.insert("Test" \ "Standard" \ "Test" \ (i, "hello:"+i))
        if( session.get("Test" \ "Standard" \ "Test" \ i) == None ) {
          onLowPrecisionSystem = true
        }
      }
      onLowPrecisionSystem
    }

    if( checkLowResolution ) {
      println("You have low resolution timer on this system")
      Utils.COMPENSATE_FOR_LOW_PRECISION_SYSTEM_TIME = true
      assertFalse("setting Utils.COMPENSATE_FOR_LOW_PRECISION_SYSTEM_TIME = true did not work around the low resolution timer problems.", checkLowResolution);
    } else {
      println("You have high resolution timer on this system")
    }
  }

}