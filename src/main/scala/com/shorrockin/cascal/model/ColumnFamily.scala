package com.shorrockin.cascal.model

import org.apache.cassandra.thrift.ColumnParent

/**
 * high level abstraction for a column family. can be considered the 2nd
 * level dimension of the cassandra data model. Column families can either
 * be Standard or Super.
 *
 * @author Chris Shorrock
 * @param KeyType the object which is used as a key for this family
 */
trait ColumnFamily[+KeyType] extends StringValue {
  val keyspace:Keyspace
  lazy val columnParent = new ColumnParent().setColumn_family(value)
  
  def \(value:String):KeyType

}