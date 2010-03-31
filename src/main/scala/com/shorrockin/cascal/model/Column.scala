package com.shorrockin.cascal.model

/**
 * defines a column object which is generally super or standard type.
 *
 * @author Chris Shorrock
 * @type Contents determines the type of object returned when this
 * column is looked up through the session get method.
 */
trait Column[Contents] extends ByteValue {
  val key:Key[_, _]
  val keyspace:Keyspace
  val family:ColumnFamily[_]
  
}