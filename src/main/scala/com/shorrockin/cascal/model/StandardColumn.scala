package com.shorrockin.cascal.model

/**
 * defines a basic column type in cassandra which is either a child of a
 * super column, or a child of a StandardKey
 */
case class StandardColumn[ColumnOwner <: ColumnContainer](val value:Array[Byte], val owner:ColumnOwner) extends ColumnName() {
  def \(value:Array[Byte]) = new ColumnValue(this, value, System.currentTimeMillis)
  def \(value:Array[Byte], ts:Long) = new ColumnValue(this, value, ts)

  val key = owner.key
  val family = key.family
  val keyspace = key.keyspace

}