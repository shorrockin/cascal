package com.shorrockin.cascal.model

/**
 * abstraction of a super column family.
 * @author Chris Shorrock
 */
case class SuperColumnFamily(val value:String, val keyspace:Keyspace) extends ColumnFamily {
  def \(value:String) = new SuperKey(value, this)
}