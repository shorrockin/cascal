package com.shorrockin.cascal;

import testing._
import session._
import utils.{UUID, Conversions}
import org.junit.{Assert, Test}
import com.shorrockin.cascal.utils.Logging

class TestLogin extends Logging {
  import Conversions._
  import Assert._  

  @Test def testValidLogin {
    val host = Host("localhost", 9160, 10000)
    val session = new Session(host, Consistency.One, true)

    // as long as we don't throw an exception - all is good
    try {
      log.debug("opening connection to cassandra server")
      session.open()

      log.debug("executing login request")
      session.login("AuthTest", "cshorrock", "thisisnotmyrealpassword")

      log.debug("executing simple count request")
      val count = session.count("AuthTest" \ "Standard" \ UUID())
      assertEquals(0, count)
    } finally {
      session.close()
    }
  }

  @Test def tetsNoLogin() {
    
  }
}