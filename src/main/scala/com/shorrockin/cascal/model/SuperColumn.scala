package com.shorrockin.cascal.model

import org.apache.cassandra.thrift.{ColumnPath, ColumnParent, ColumnOrSuperColumn}
import scala.collection.jcl.Conversions.convertList

/**
 * a super standard key the key who's parent is a super key. It acts in much
 * the same fashion as a standard key except for the parent structure is a SuperKey,
 * and not a StandardColumnFamily
 *
 * @author Chris Shorrock
 */
case class SuperColumn(val value:Array[Byte], val key:SuperKey) extends Gettable[Seq[Column[SuperColumn]]]()
                                                                   with StandardColumnContainer[Column[SuperColumn], Seq[Column[SuperColumn]]] {
  def \(name:Array[Byte]) = new Column(name, this)
  def \(name:Array[Byte], value:Array[Byte]) = new Column(name, value, this)
  def \(name:Array[Byte], value:Array[Byte], time:Long) = new Column(name, value, time, this)

  val family = key.family
  val keyspace = family.keyspace

  lazy val columnParent = new ColumnParent(family.value).setSuper_column(value)
  lazy val columnPath = new ColumnPath(family.value).setSuper_column(value)

  /**
   * given the returned object from the get request, convert
   * to our return type.
   */
  def convertGetResult(colOrSuperCol:ColumnOrSuperColumn):Seq[Column[SuperColumn]] = {
    val superCol = colOrSuperCol.getSuper_column
    convertList(superCol.getColumns).map { (column) => \(column.getName, column.getValue, column.getTimestamp) }
  }


  /**
   * given the return object from the list request, convert it to
   * our return type
   */
  def convertListResult(results:Seq[ColumnOrSuperColumn]):Seq[Column[SuperColumn]] = {
    results.map { (result) =>
      val column = result.getColumn
      \(column.getName, column.getValue, column.getTimestamp)
    }
  }
}