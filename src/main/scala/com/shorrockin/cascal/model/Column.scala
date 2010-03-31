package com.shorrockin.cascal.model

import java.util.Date
import org.apache.cassandra.thrift.ColumnOrSuperColumn

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


  /**
   * copy method to create a new instance of this column with a new value and
   * the same other values.
   */
  def \(newValue:Array[Byte]) = new Column[Owner](name, newValue, time, owner)


  /**
   * given the cassandra object returned from retrieving this object,
   * returns an instance of our return type.
   */
  def convertGetResult(colOrSuperCol:ColumnOrSuperColumn):Column[Owner] = {
    val col = colOrSuperCol.getColumn
    Column(col.getName, col.getValue, col.getTimestamp, owner)
  }

}