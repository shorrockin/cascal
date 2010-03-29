package com.shorrockin.cascal.model

case class SuperKey(val value:String, val family:SuperColumnFamily) extends Key[SuperColumn] {
  def \(value:Array[Byte]) = new SuperColumn(value, this)
}