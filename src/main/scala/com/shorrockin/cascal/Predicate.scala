package com.shorrockin.cascal

import org.apache.cassandra.thrift.SlicePredicate

/**
 * a predicate defines a function that is applied to a result set to limit
 * the results returned.
 *
 * @author Chris Shorrock
 */
trait Predicate {
  val slicePredicate:SlicePredicate
}