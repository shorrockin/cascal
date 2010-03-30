package com.shorrockin.cascal.model

/**
 * defines a basic column type in cassandra which is either a child of a
 * super column, or a child of a StandardKey
 */
case class StandardColumn[Owner](val value:Array[Byte], val owner:Owner) extends ColumnName[ColumnValue[Owner]]() {
  def \(value:Array[Byte])          = new ColumnValue[Owner](this, value, System.currentTimeMillis)
  def \(value:Array[Byte], ts:Long) = new ColumnValue[Owner](this, value, ts)

  val key = owner.asInstanceOf[ColumnContainer[StandardKey, _]].key
  val family = key.family
  val keyspace = key.keyspace

}