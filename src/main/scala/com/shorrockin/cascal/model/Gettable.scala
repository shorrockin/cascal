package com.shorrockin.cascal.model

/**
 * defines an object which can be looked up through a session get
 * method.
 *
 * @author Chris Shorrock
 * @type contents determines the type of object returned when this
 * column is looked up through the session get method.
 */
trait Gettable[Contents] extends ByteValue {
  val key:Key[_, _]
  val keyspace:Keyspace
  val family:ColumnFamily[_]
  
}