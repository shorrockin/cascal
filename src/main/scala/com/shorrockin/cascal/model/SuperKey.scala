package com.shorrockin.cascal.model

import scala.collection.jcl.Conversions.convertList
import org.apache.cassandra.thrift.{ColumnOrSuperColumn}

case class SuperKey(val value:String, val family:SuperColumnFamily) extends Key[SuperColumn, Seq[(SuperColumn, Seq[Column[SuperColumn]])]] {

  def \(value:Array[Byte]) = new SuperColumn(value, this)

  /**
   *  converts a list of super columns to the specified return type
   */
  def convertListResult(results:Seq[ColumnOrSuperColumn]):Seq[(SuperColumn, Seq[Column[SuperColumn]])] = {
    results.map { (result) =>
      val nativeSuperCol = result.getSuper_column
      val superColumn    = this \ nativeSuperCol.getName
      val columns = convertList(nativeSuperCol.getColumns).map { (column) =>
        superColumn \ (column.getName, column.getValue, column.getTimestamp)
      }
      (superColumn -> columns)
    }
  }

  override def toString = "%s \\ SuperKey(value = %s)".format(family.toString, value)
}