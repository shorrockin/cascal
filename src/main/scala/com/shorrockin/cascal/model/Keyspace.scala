package com.shorrockin.cascal.model

/**
 * provides the high level abstraction for the keyspace. can be thought
 * of as a DB schema. also can be considered the 1st dimension of the
 * cassandra map. This class can be used in the following ways to construct
 * paths to various endpoints in the cassandra namespace.:
 *
 * "ExampleKeyspace" \\ "TheSuperFamily" \ "SuperKey" \ "StandardKey"
 * "ExampleKeyspace" \ "ColumnFamily" \ "Key"
 *
 * @author Chris Shorrock
 */
case class Keyspace(val value:String) {
  def \(value:String):StandardColumnFamily = new StandardColumnFamily(value, this)
  def \\(value:String):SuperColumnFamily = new SuperColumnFamily(value, this)
}