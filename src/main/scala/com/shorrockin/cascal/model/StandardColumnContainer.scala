package com.shorrockin.cascal.model

/**
 * a type of column container which holds standard columns.
 *
 * @author Chris Shorrock
 */
trait StandardColumnContainer[ColumnType, SliceType] extends ColumnContainer[ColumnType, SliceType] {
  def \(name:Array[Byte]):ColumnType
  def \(name:Array[Byte], value:Array[Byte]):ColumnType
  def \(name:Array[Byte], value:Array[Byte], time:Long):ColumnType  
}
