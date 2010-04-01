package com.shorrockin.cascal

import me.prettyprint.cassandra.testutils.EmbeddedServerHelper

/**
 * trait which mixes in the functionality necessary to embed
 * cassandra into a unit test
 */
trait EmbeddedCassandra {
  def boot(f:(Session) => Unit) = {
    val embedded                = new EmbeddedServerHelper
    var session:Option[Session] = None

    try {
      embedded.setup
      session = Some(new Session("localhost", 9160, Consistency.One))
      f(session.get)
    } finally {
      if (session.isDefined) session.get.close
      embedded.shutdown
    }
  }


}