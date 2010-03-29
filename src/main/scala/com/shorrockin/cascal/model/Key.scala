package com.shorrockin.cascal.model

/**
 * a key is an abstract type which maps to either a standard key which contains
 * a collection of columns, or a super key, which contains a collection of
 * standard keys.
 */
trait Key[ColumnType] extends StringValue with ColumnContainer[ColumnType] {
  val value:String

  val keyspace = family.keyspace
  val key = this
}