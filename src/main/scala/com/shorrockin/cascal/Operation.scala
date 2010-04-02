package com.shorrockin.cascal

import model.Column
import org.apache.cassandra.thrift.Mutation

/**
 * defines an operation that can be executed in parallel with a collection
 * of other operations
 *
 * @author Chris Shorrock
 */
trait Operation {
  val mutation:Mutation
  def ::(op:Operation) = this :: op :: Nil
}

/**
 * defines an operation of type insertion that can be used in batch mode
 * to insert a collection of mutations at once.
 *
 * @author Chris Shorrock
 */
case class Insertion(val column:Column[_]) extends Operation {
  val mutation = new Mutation
}


/**
 * defines a delete operation which removes a column, or even a collection
 * of columns when used on a super column.
 *
 * @author Chris Shorrock
 */
case class Deletion() extends Operation {
  val mutation = new Mutation
}