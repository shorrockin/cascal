package com.shorrockin.cascal.model

/**
 * defines a basic column type in cassandra which is either a child of a
 * super column, or a child of a StandardKey
 */
case class StandardColumn[E](val value:Array[Byte], val owner:E) extends ColumnName[ColumnValue[E]]() {
  def \(value:Array[Byte])          = new ColumnValue[E](this, value, System.currentTimeMillis)
  def \(value:Array[Byte], ts:Long) = new ColumnValue[E](this, value, ts)

  val key = owner.asInstanceOf[ColumnContainer[StandardKey]].key
  val family = key.family
  val keyspace = key.keyspace

}