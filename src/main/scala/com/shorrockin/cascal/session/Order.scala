package com.shorrockin.cascal.session

/**
 * defines the various ordering options available.
 *
 * @author Chris Shorrock
 */
object Order {
  val Ascending = new Order { def reversed = false }
  val Descending = new Order { def reversed = true }
}

/**
 * trait for the cassandra order types.
 *
 * @author Chris Shorrock
 */
trait Order {
  def reversed:Boolean
}