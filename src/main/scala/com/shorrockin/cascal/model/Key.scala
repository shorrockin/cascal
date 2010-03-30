package com.shorrockin.cascal.model

/**
 * a key is an abstract type which maps to either a standard key which contains
 * a collection of columns, or a super key, which contains a collection of
 * standard keys.
 *
 * @type ColumnType the type of column that this key holds
 * @type SliceType when this key is used in a slice the type of object that is returned
 */
trait Key[ColumnType, SliceType] extends StringValue 
                                    with ColumnContainer[ColumnType, SliceType] {
  val value:String

  val keyspace = family.keyspace
  val key = this
}