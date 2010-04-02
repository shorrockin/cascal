package com.shorrockin.cascal

import model._
import org.apache.cassandra.thrift.{Deletion, Mutation}

/**
 * defines an operation that can be executed in parallel with a collection
 * of other operations
 *
 * @author Chris Shorrock
 */
trait Operation {
  val mutation:Mutation
  val family:ColumnFamily[_]
  val key:Key[_, _]
  val keyspace:Keyspace
  def ::(op:Operation) = this :: op :: Nil
}

/**
 * defines an operation of type insertion that can be used in batch mode
 * to insert a collection of mutations at once.
 *
 * @author Chris Shorrock
 */
case class Insert(val column:Column[_]) extends Operation {
  lazy val mutation = new Mutation().setColumn_or_supercolumn(column.columnOrSuperColumn)
  val family = column.family
  val key = column.key
  val keyspace = column.keyspace
}


case object Delete {
  def apply(container:ColumnContainer[_, _], predicate:Predicate) = new Delete(container, predicate)
  def apply(container:ColumnContainer[_, _]) = new Delete(container, EmptyPredicate)
}

/**
 * defines a delete operation which removes a column, or even a collection
 * of columns when used on a super column.
 *
 * @author Chris Shorrock
 */
class Delete(val container:ColumnContainer[_, _], val predicate:Predicate) extends Operation {

  lazy val mutation = {
    val out = new Mutation
    val del = new Deletion
    del.setTimestamp(System.currentTimeMillis)

    predicate match {
      case EmptyPredicate => /* do nothing */
      case _ => del.setPredicate(predicate.slicePredicate)
    }

    container match {
      case sc:SuperColumn => del.setSuper_column(sc.value)
      case _ => /* ignore */
    }

    out.setDeletion(del)
  }
  
  val family = container.family
  val key = container.key
  val keyspace = container.keyspace
}