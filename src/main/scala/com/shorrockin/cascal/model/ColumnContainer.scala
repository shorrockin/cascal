package com.shorrockin.cascal.model

/**
 * defines a cassandra object which houses a collection of columns. Generally
 * this will be either a StandardKey, a SuperKey, or a SuperColumn. As each of
 * these house columns, with the SuperKey housing SuperColumns and the other two
 * housing DepStandardColumn.
 *
 * @type ColumnType the type of columns that this container houses.
 * @type SliceType when sliced, what type of object does it return.
 */
trait ColumnContainer[ColumnType, SliceType] {
  def \(value:Array[Byte]):ColumnType

  val family:ColumnFamily[_]
  val key:Key[_, _]
  val keyspace:Keyspace
}
