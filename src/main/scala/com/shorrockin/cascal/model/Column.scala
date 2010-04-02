package com.shorrockin.cascal.model

import java.util.Date
import org.apache.cassandra.thrift.{ColumnPath, ColumnOrSuperColumn}

/**
 * a column is the child component of a super column or a
 * standard key
 */
case class Column[Owner](val name:Array[Byte],
                         val value:Array[Byte],
                         val time:Long,
                         val owner:Owner) extends Gettable[Column[Owner]] {

  def this(name:Array[Byte], value:Array[Byte], owner:Owner) = this(name, value, System.currentTimeMillis, owner)
  def this(name:Array[Byte], owner:Owner) = this(name, null, System.currentTimeMillis, owner)
  def this(name:Array[Byte], value:Array[Byte], date:Date, owner:Owner) = this(name, value, date.getTime, owner)


  val partial  = (value == null)
  val key      = owner.asInstanceOf[ColumnContainer[_, _]].key
  val family   = key.family
  val keyspace = key.keyspace

  lazy val columnPath = {
    val out = new ColumnPath(family.value)
    owner match {
      case owner:SuperColumn => out.setColumn(name).setSuper_column(owner.value)
      case key:StandardKey   => out.setColumn(name)
    }
  }


  /**
   * copy method to create a new instance of this column with a new value and
   * the same other values.
   */
  def \(newValue:Array[Byte]) = new Column[Owner](name, newValue, time, owner)


  /**
   * appends a column onto this one forming a list
   */
  def ::(other:Column[Owner]):List[Column[Owner]] = other :: this :: Nil


  /**
   *  given the cassandra object returned from retrieving this object,
   * returns an instance of our return type.
   */
  def convertGetResult(colOrSuperCol:ColumnOrSuperColumn):Column[Owner] = {
    val col = colOrSuperCol.getColumn
    Column(col.getName, col.getValue, col.getTimestamp, owner)
  }

}