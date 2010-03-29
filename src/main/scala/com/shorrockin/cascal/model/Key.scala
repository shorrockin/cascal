package com.shorrockin.cascal.model

/**
 * a key is an abstract type which maps to either a standard key which contains
 * a collection of columns, or a super key, which contains a collection of
 * standard keys.
 */
abstract case class Key() extends ColumnContainer {
  val family:ColumnFamily
  val keyspace = family.keyspace
  val value:String
  val key = this
}