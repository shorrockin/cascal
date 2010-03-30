package com.shorrockin.cascal.model

case class SuperKey(val value:String, val family:SuperColumnFamily) extends Key[SuperColumn, Map[SuperColumn, Seq[ColumnValue[SuperColumn]]]] {
  def \(value:Array[Byte]) = new SuperColumn(value, this)
}