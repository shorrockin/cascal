package com.shorrockin.cascal

import org.apache.cassandra.thrift.{SliceRange, SlicePredicate}

/**
 * a predicate defines a function that is applied to a result set to limit
 * the results returned.
 *
 * @author Chris Shorrock
 */
trait Predicate {
  val slicePredicate:SlicePredicate
}


/**
 * defines a column predicate which is a type of predicate which limits
 * the results to the columns specified
 */
case class ColumnPredicate(values:Seq[Array[Byte]]) extends Predicate {
  val slicePredicate = new SlicePredicate()
  slicePredicate.setColumn_names(Conversions.toJavaList(values))
}

object RangePredicate {
  def apply(limit:Int) = new RangePredicate(None, None, Order.Ascending, Some(limit))
  def apply(order:Order, limit:Int) = new RangePredicate(None, None, order, Some(limit))
  def apply(start:Array[Byte], end:Array[Byte]) = new RangePredicate(Some(start), Some(end), Order.Ascending, None)
  def apply(start:Array[Byte], end:Array[Byte], limit:Int) = new RangePredicate(Some(start), Some(end), Order.Ascending, Some(limit))
  def apply(start:Option[Array[Byte]], end:Option[Array[Byte]], order:Order, limit:Option[Int]) = new RangePredicate(start, end, order, limit)
}

/**
 * a type of predicate which allows you to specify a range of values
 */
class RangePredicate(start:Option[Array[Byte]], end:Option[Array[Byte]], order:Order, limit:Option[Int]) extends Predicate {
  val emptyBytes = new Array[Byte](0)

  def optBytesToBytes(opt:Option[Array[Byte]]) = opt match {
    case None        => emptyBytes
    case Some(array) => array
  }

  val limitVal = limit match {
    case None    => Integer.MAX_VALUE
    case Some(i) => i
  }

  val slicePredicate = new SlicePredicate()
  slicePredicate.setSlice_range(new SliceRange(optBytesToBytes(start), optBytesToBytes(end), order.reversed, limitVal))
}

case object EmptyPredicate extends RangePredicate(None, None, Order.Ascending, None)