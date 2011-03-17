package com.shorrockin.cascal.model

import java.nio.ByteBuffer
import java.util.Date
import org.apache.cassandra.thrift.{ColumnPath, ColumnOrSuperColumn}
import org.apache.cassandra.thrift.{Column => CassColumn}
import org.apache.cassandra.thrift.{SuperColumn => CassSuperColumn}
import com.shorrockin.cascal.utils.Conversions
import com.shorrockin.cascal.utils.Utils.now


/**
 * a column is the child component of a super column or a
 * standard key.
 *
 * @author Chris Shorrock
 * @param Owner the type of object which owns this column
 */
case class Column[Owner](val name:ByteBuffer,
                         val value:ByteBuffer,
                         val time:Long,
                         val owner:Owner) extends Gettable[Column[Owner]] {

  def this(name:ByteBuffer, value:ByteBuffer, owner:Owner) = this(name, value, now, owner)
  def this(name:ByteBuffer, owner:Owner) = this(name, null, now, owner)
  def this(name:ByteBuffer, value:ByteBuffer, date:Date, owner:Owner) = this(name, value, date.getTime, owner)


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

  lazy val columnOrSuperColumn = {
    val cosc = new ColumnOrSuperColumn
    owner match {
      case key:StandardKey => cosc.setColumn(new CassColumn(name, value, time))
      case sup:SuperColumn =>
        val list = Conversions.toJavaList(new CassColumn(name, value, time) :: Nil)
        cosc.setSuper_column(new CassSuperColumn(sup.value, list))
    }
  }


  /**
   * copy method to create a new instance of this column with a new value and
   * the same other values.
   */
  def \(newValue:ByteBuffer) = new Column[Owner](name, newValue, time, owner)


  /**
   * appends a column onto this one forming a list
   */
  def ::(other:Column[Owner]):List[Column[Owner]] = other :: this :: Nil


  /**
   * given the cassandra object returned from retrieving this object,
   * returns an instance of our return type.
   */
  def convertGetResult(colOrSuperCol:ColumnOrSuperColumn):Column[Owner] = {
    val col = colOrSuperCol.getColumn
    Column(ByteBuffer.wrap(col.getName), ByteBuffer.wrap(col.getValue), col.getTimestamp, owner)
  }

  private def stringIfPossible(a:ByteBuffer):String = {
    if (a == null) return "NULL"
    if (a.array.length <= 4) return "Array (" + a.array.mkString(", ") + ")"
    if (a.array.length > 1000) return a.array.toString
    try { Conversions.string(a) } catch { case _ => a.array.toString }
  }

  override def toString():String = "%s \\ Column(name = %s, value = %s, time = %s)".format(
      owner.toString, stringIfPossible(name), stringIfPossible(value), time)
}
