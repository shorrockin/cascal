package com.shorrockin.cascal.model

import java.nio.ByteBuffer

/**
 * a type of column container which holds standard columns.
 *
 * @author Chris Shorrock
 */
trait StandardColumnContainer[ColumnType, SliceType] extends ColumnContainer[ColumnType, SliceType] {
  def \(name:ByteBuffer):ColumnType
  def \(name:ByteBuffer, value:ByteBuffer):ColumnType
  def \(name:ByteBuffer, value:ByteBuffer, time:Long):ColumnType
}
