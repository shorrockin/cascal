package com.shorrockin.cascal.model

trait StandardColumnContainer[ColumnType, SliceType] extends ColumnContainer[ColumnType, SliceType] {
  def \(name:Array[Byte]):ColumnType
  def \(name:Array[Byte], value:Array[Byte]):ColumnType
  def \(name:Array[Byte], value:Array[Byte], time:Long):ColumnType  
}
