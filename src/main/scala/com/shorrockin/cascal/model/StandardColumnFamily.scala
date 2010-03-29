package com.shorrockin.cascal.model

/**
 * abstraction for the standard column family. a standard column family
 * contains a collection of keys each mapped to a collection of columns.
 *
 * @author Chris Shorrock
 */
case class StandardColumnFamily(val value:String, val keyspace:Keyspace) extends ColumnFamily[StandardKey] {
  def \(value:String) = new StandardKey(value, this)
}