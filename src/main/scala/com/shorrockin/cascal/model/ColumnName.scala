package com.shorrockin.cascal.model

/**
 * defines a cassandra column, can be of super or standard type.
 *
 * @author Chris Shorrock
 */
abstract class ColumnName() {
  val value:Array[Byte]
  val key:Key
}