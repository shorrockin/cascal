package com.shorrockin.cascal.model

/**
 * a column family which houses super columns
 *
 * @author Chris Shorrock
 */
case class SuperColumnFamily(val value:String, val keyspace:Keyspace) extends ColumnFamily[SuperKey] {
  def \(value:String) = new SuperKey(value, this)
  override def toString = "%s \\\\ SuperColumnFamily(value = %s)".format(keyspace.toString, value)
}