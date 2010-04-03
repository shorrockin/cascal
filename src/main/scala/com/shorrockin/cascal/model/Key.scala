package com.shorrockin.cascal.model

import org.apache.cassandra.thrift.{ColumnParent, ColumnPath}

/**
 * a key is an abstract type which maps to either a standard key which contains
 * a collection of columns, or a super key, which contains a collection of
 * standard keys.
 *
 * @author Chris Shorrock
 * @param ColumnType the type of column that this key holds
 * @param ListType when this key is used in a list the type of object that is returned
 */
trait Key[ColumnType, ListType] extends StringValue with ColumnContainer[ColumnType, ListType] {
  val value:String

  val keyspace = family.keyspace
  val key = this

  lazy val columnPath = new ColumnPath(family.value)
  lazy val columnParent = new ColumnParent(family.value)

  def ::(other:Key[ColumnType, ListType]):List[Key[ColumnType, ListType]] = other :: this :: Nil
}