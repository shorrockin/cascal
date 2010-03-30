package com.shorrockin.cascal

import org.apache.cassandra.thrift.SlicePredicate

/**
 * defines a column predicate which is a type of predicate which limits
 * the results to the columns specified
 */
case class ColumnPredicate(values:Seq[Array[Byte]]) extends Predicate {
  val slicePredicate = new SlicePredicate()
  slicePredicate.setColumn_names(Conversions.toJavaList(values))
}