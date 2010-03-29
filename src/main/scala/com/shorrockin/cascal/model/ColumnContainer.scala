package com.shorrockin.cascal.model

/**
 * defines a cassandra object which houses a collection of columns. Generally
 * this will be either a StandardKey, or a SuperColumn.
 */
trait ColumnContainer {
  val family:ColumnFamily
  val key:Key
  val keyspace:Keyspace
}
