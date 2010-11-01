package com.shorrockin.cascal.utils

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * simple logging trait to access the commons logging trait.
 *
 * @author Chris Shorrock
 */
trait Logging {
  lazy val log = LoggerFactory.getLogger(this.getClass.getName)
}
