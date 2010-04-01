package com.shorrockin.cascal

import org.apache.commons.logging.LogFactory

trait Logging {
  @transient @volatile lazy val log = LogFactory.getLog(this.getClass.getName)
}
