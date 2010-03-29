package com.shorrockin.cascal.model

/**
 * defines a cassandra object which houses a collection of columns. Generally
 * this will be either a StandardKey, a SuperKey, or a SuperColumn. As each of
 * these house columns, with the SuperKey housing SuperColumns and the other two
 * housing StandardColumn
 */
trait ColumnContainer[ColumnType] {
  val family:ColumnFamily[_]
  val key:Key[_]
  val keyspace:Keyspace
}
