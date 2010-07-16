package com.shorrockin.cascal.utils

import org.apache.commons.logging.LogFactory

/**
 * simple logging trait to access the commons logging trait.
 *
 * @author Chris Shorrock
 */
trait Logging {
  lazy val log = LogFactory.getLog(this.getClass.getName)
}
