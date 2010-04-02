package com.shorrockin.cascal.model

import scala.collection.jcl.Conversions.convertList
import org.apache.cassandra.thrift.{ColumnOrSuperColumn}

case class SuperKey(val value:String, val family:SuperColumnFamily) extends Key[SuperColumn, Map[SuperColumn, Seq[Column[SuperColumn]]]] {

  def \(value:Array[Byte]) = new SuperColumn(value, this)

  /**
   *  converts a list of super columns to the specified return type
   */
  def convertListResult(results:Seq[ColumnOrSuperColumn]):Map[SuperColumn, Seq[Column[SuperColumn]]] = {
    var map = Map[SuperColumn, Seq[Column[SuperColumn]]]()
    results.foreach { (result) =>
      val nativeSuperCol = result.getSuper_column
      val superColumn    = this \ nativeSuperCol.getName
      val columns = convertList(nativeSuperCol.getColumns).map { (column) =>
        superColumn \ (column.getName, column.getValue, column.getTimestamp)
      }
      map = map + (superColumn -> columns)
    }
    map
  }
}