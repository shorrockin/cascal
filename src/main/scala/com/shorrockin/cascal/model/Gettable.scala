package com.shorrockin.cascal.model

import org.apache.cassandra.thrift.{ColumnPath, ColumnOrSuperColumn}

/**
 * defines an object which can be looked up through a session get
 * method.
 *
 * @author Chris Shorrock
 * @type Result determines the type of object returned when this
 * column is looked up through the session get method.
 */
trait Gettable[Result] extends ByteValue {
  val key:Key[_, _]
  val keyspace:Keyspace
  val family:ColumnFamily[_]
  val columnPath:ColumnPath

  def convertGetResult(colOrSuperCol:ColumnOrSuperColumn):Result


}