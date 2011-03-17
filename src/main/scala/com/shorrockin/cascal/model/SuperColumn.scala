package com.shorrockin.cascal.model

import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{ColumnPath, ColumnParent, ColumnOrSuperColumn}
import com.shorrockin.cascal.utils.Conversions

/**
 * a super standard key the key who's parent is a super key. It acts in much
 * the same fashion as a standard key except for the parent structure is a SuperKey,
 * and not a StandardColumnFamily
 *
 * @author Chris Shorrock
 */
case class SuperColumn(val value:ByteBuffer, val key:SuperKey) extends Gettable[Seq[Column[SuperColumn]]]()
                                                                   with StandardColumnContainer[Column[SuperColumn], Seq[Column[SuperColumn]]] {
  def \(name:ByteBuffer) = new Column(name, this)
  def \(name:ByteBuffer, value:ByteBuffer) = new Column(name, value, this)
  def \(name:ByteBuffer, value:ByteBuffer, time:Long) = new Column(name, value, time, this)

  val family = key.family
  val keyspace = family.keyspace

  lazy val columnParent = new ColumnParent(family.value).setSuper_column(value)
  lazy val columnPath = new ColumnPath(family.value).setSuper_column(value)

  def ::(other:SuperColumn):List[SuperColumn] = other :: this :: Nil

  private def convertList[T](v:java.util.List[T]):List[T] = {
	 scala.collection.JavaConversions.asBuffer(v).toList
  }

  /**
   * given the returned object from the get request, convert
   * to our return type.
   */
  def convertGetResult(colOrSuperCol:ColumnOrSuperColumn):Seq[Column[SuperColumn]] = {
    val superCol = colOrSuperCol.getSuper_column
    convertList(superCol.getColumns).map { (column) => \(ByteBuffer.wrap(column.getName), ByteBuffer.wrap(column.getValue), column.getTimestamp) }
  }


  /**
   * given the return object from the list request, convert it to
   * our return type
   */
  def convertListResult(results:Seq[ColumnOrSuperColumn]):Seq[Column[SuperColumn]] = {
    results.map { (result) =>
      val column = result.getColumn
      \(ByteBuffer.wrap(column.getName), ByteBuffer.wrap(column.getValue), column.getTimestamp)
    }
  }

  private def stringIfPossible(a:ByteBuffer):String = {
    if (a.array.length <= 4) return "Array (" + a.array.mkString(", ") + ")"
    if (a.array.length > 1000) return a.array.toString
    try { Conversions.string(a) } catch { case _ => a.array.toString }
  }

  override def toString():String = "%s \\ SuperColumn(value = %s)".format(
      key.toString, stringIfPossible(value))
}
