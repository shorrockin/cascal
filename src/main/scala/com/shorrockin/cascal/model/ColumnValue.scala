package com.shorrockin.cascal.model

import java.util.Date

case class ColumnValue[ColumnOwner <: ColumnContainer](name:StandardColumn[ColumnOwner], value:Array[Byte], time:Long) {
  def this(name:StandardColumn[ColumnOwner], value:Array[Byte]) = this(name, value, System.currentTimeMillis)
  def this(name:StandardColumn[ColumnOwner], value:Array[Byte], date:Date) = this(name, value, date.getTime)

  val owner = name.owner
  val family = name.family
  val keyspace = name.keyspace
  val key = name.key
}