package com.shorrockin.cascal.model

import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{ColumnOrSuperColumn}

/**
 * implementation of a standard key, which is an object which can be thought
 * of as a container for a list of columns. The parent of this will either
 * be a StandardColumnFamily or a SuperKey
 *
 * @author Chris Shorrock
 */
case class StandardKey(val value:String, val family:StandardColumnFamily) extends Key[Column[StandardKey], Seq[Column[StandardKey]]]
                                                                             with StandardColumnContainer[Column[StandardKey], Seq[Column[StandardKey]]] {

  def \(name:ByteBuffer) = new Column(name, this)
  def \(name:ByteBuffer, value:ByteBuffer) = new Column(name, value, this)
  def \(name:ByteBuffer, value:ByteBuffer, time:Long) = new Column(name, value, time, this)

  def convertListResult(results:Seq[ColumnOrSuperColumn]):Seq[Column[StandardKey]] = {
    results.map { (result) =>
      val column = result.getColumn
      \(ByteBuffer.wrap(column.getName), ByteBuffer.wrap(column.getValue), column.getTimestamp)
    }
  }

  override def toString = "%s \\ StandardKey(value = %s)".format(family.toString, value)
}
