package com.shorrockin.cascal.model

/**
 * high level abstraction for a column family. can be considered the 2nd
 * level dimension of the cassandra data model. Calumn families can either
 * be Standard or Super.
 */
trait ColumnFamily[KeyType] extends StringValue {
  val keyspace:Keyspace
  def \(value:String):KeyType
}