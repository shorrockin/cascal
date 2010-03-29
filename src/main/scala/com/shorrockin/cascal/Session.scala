package com.shorrockin.cascal


import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import collection.jcl.Conversions._
import java.util.Arrays
import java.util.{List => JList}
import com.shorrockin.cascal.Conversions._

import model._
import org.apache.cassandra.thrift.{Cassandra, ColumnPath, ColumnParent, SliceRange, SlicePredicate, ColumnOrSuperColumn, ConsistencyLevel}

/**
 * a cascal session is the entry point for interacting with the
 * cassandra system through various path elements.
 *
 * @author Chris Shorrock
 */
class Session(val host:String, val port:Int, val defaultConsistency:Consistency) {

  // TODO - replace with a better way to retrieve the connection
  val sock    = new TSocket(host, port);
  val tr      = new TBinaryProtocol(sock);
  val client  = new Cassandra.Client(tr,tr);
  sock.open();
  def close() = sock.close();

  /**
   * return the current cluster name of the cassandra instance
   */
  def clusterName = client.get_string_property("cluster name")


  /**
   * returns the configuration file of the connected cassandra instance
   */
  def configFile = client.get_string_property("config file")


  /**
   * returns the version of the cassandra instance
   */
  def version = client.get_string_property("version")


  /**
   * returns a map of tokens from the cassandra instance.
   */
  def tokenMap = Map("1" -> "2", "3" -> "4") // in the form of {"token1":"host1","token2":"host2"}


  /**
   * returns all the keyspaces from the cassandra instance
   */
  def keyspaces = client.get_string_list_property("keyspaces")


  /**
   *  returns the column value for the specified column
   */
  def get[E](col:ColumnName[E], consistency:Consistency):E = {
    toColumnNameType(col, client.get(col.keyspace.value, col.key.value, toColumnPath(col), consistency))
  }

  
  /**
   * returns the column value for the specified column, using the default consistency
   */
  def get[E](col:ColumnName[E]):E = get(col, defaultConsistency)


  /**
   * inserts the specified column value
   */
  def insert(value:ColumnValue[_], consistency:Consistency) {
    client.insert(value.keyspace.value, value.key.value, toColumnPath(value.name), value.value, value.time, consistency)
  }



  /**
   * inserts the specified column value using the default consistency
   */
  def insert(value:ColumnValue[_]):Unit = insert(value, defaultConsistency)
//
//
//  /**
//   * counts the number of columns in the specified column container
//   */
//  def count(container:ColumnContainer, consistency:Consistency):Int = {
//    client.get_count(container.keyspace.value, container.key.value, containerToParent(container), consistency)
//  }
//
//
//  /**
//   * performs count on the specified column container
//   */
//  def count(container:ColumnContainer):Int = count(container, defaultConsistency)
//
//
//  /**
//   * performs a list of the provided standard key. uses the list of columns as the predicate
//   * to determine which columns to return.
//   */
//  def list(key:StandardKey, includeColumns:Seq[Array[Byte]], consistency:Consistency):Seq[ColumnValue[StandardKey]] = {
//    val results = client.get_slice(key.keyspace.value, key.key.value, containerToParent(key), includeColumns, consistency)
//    columnResultListToValueSeq(key, results)
//  }
//
//
//  /**
//   * performs a list of the columns for the specified key. Takes in options that determine how
//   * this list can be filtered.
//   */
//  def list(key:StandardKey, start:Option[Array[Byte]], end:Option[Array[Byte]], order:Order, limit:Option[Int], consistency:Consistency):Seq[ColumnValue[StandardKey]] = {
//    val results = client.get_slice(key.keyspace.value, key.key.value, containerToParent(key), toRangePredicate(start, end, order, limit), consistency)
//    columnResultListToValueSeq(key, results)
//  }
//
//
//  /**
//   * performs a list of the columns for the specified key, Takes in options which determine how this
//   * list can be filtered. Uses the default consistency option.
//   */
//  def list(key:StandardKey, start:Option[Array[Byte]], end:Option[Array[Byte]], order:Order, limit:Option[Int]):Seq[ColumnValue[StandardKey]] = {
//    list(key, start, end, order, limit, defaultConsistency)
//  }
//
//
//  /**
//   * performs a list of the provided standard key, uses the included list to determine the
//   * the columns to return. uses the default consistency level.
//   */
//  def list(key:StandardKey, includeColumns:Seq[Array[Byte]]):Seq[ColumnValue[StandardKey]] = list(key, includeColumns, defaultConsistency)
//
//
//  /**
//   * lists all the columns with the specified consistency
//   */
//  def list(key:StandardKey, consistency:Consistency):Seq[ColumnValue[StandardKey]] = list(key, Nil, consistency)
//
//
//  /**
//   * lists all the columns with the default consistency
//   */
//  def list(key:StandardKey):Seq[ColumnValue[StandardKey]] = list(key, Nil, defaultConsistency)


//  /**
//   * takes in a list of columnorsupercolumn objects, assumes they're all normal columns,
//   * and creates a scala collection of column values.
//   */
//  private def columnResultListToValueSeq(key:StandardKey, results:JList[ColumnOrSuperColumn]):Seq[ColumnValue[StandardKey]] = {
//    val cache = Map[Array[Byte], StandardColumn[StandardKey]]()
//
//    def lookup(value:Array[Byte]):StandardColumn[StandardKey] = {
//      if (cache.contains(value)) cache.get(value).get else (key \ value)
//    }
//
//    convertList(results).map { (cosc) =>
//      val rawColumn = cosc.getColumn
//      val column    = lookup(rawColumn.getName)
//      column.\(rawColumn.getValue, rawColumn.getTimestamp)
//    }
//  }


  /**
   * implicitly coverts a consistency value to an int
   */
  private implicit def toThriftConsistency(c:Consistency):ConsistencyLevel = c.thriftValue


  /**
   *  converts a scala sequence to a java list
   */
  private implicit def toJavaList[T](l: Seq[T]):java.util.List[T] = l.foldLeft(new java.util.ArrayList[T](l.size)){(al, e) => al.add(e); al}


  /**
   * converts a list of columns to a slice predicate.
   */
  private def toColumnPredicate(list:Seq[Array[Byte]]):SlicePredicate = list.size match {
    case 0 => toRangePredicate(None, None, Order.Ascending, None)
    case _ => val out = new SlicePredicate() ; out.setColumn_names(list)
  }


  /**
   * creates a ranged slice predicate based on the values provided
   */
  private def toRangePredicate(start:Option[Array[Byte]], end:Option[Array[Byte]], order:Order, limit:Option[Int]) = {
    def optBytesToBytes(opt:Option[Array[Byte]]) = opt match {
      case None        => emptyBytes
      case Some(array) => array
    }

    val limitVal = limit match {
      case None    => Integer.MAX_VALUE
      case Some(i) => i
    }

    val out = new SlicePredicate()
    out.setSlice_range(new SliceRange(optBytesToBytes(start), optBytesToBytes(end), order.reversed, limitVal))
  }


//
//  /**
//   * converts a column container to a parent structure
//   */
//  private def toColumnParent(col:ColumnContainer):ColumnParent = col match {
//    case key:SuperColumn => val out = new ColumnParent(col.family.value) ; out.setSuper_column(key.value)
//    case key:Key => new ColumnParent(col.family.value)
//  }


  /**
   * given the column name which was used to retrieve the result
   * create the instance of E required to interpret this result.
   */
  private def toColumnNameType[E](col:ColumnName[E], result:ColumnOrSuperColumn):E = columnToDefinition(col) match {
    // returns a ColumnValue
    case StandardColDef(value, std) =>
      val resCol = result.getColumn
      (std.\(resCol.getValue, resCol.getTimestamp)).asInstanceOf[E]

    // returns a ColumnValue
    case SuperStandardColDef(SuperColDef(superVal, sup), StandardColDef(colVal, std)) =>
      val resCol = result.getColumn
      (std.\(resCol.getValue, resCol.getTimestamp)).asInstanceOf[E]

    // returns a Map[StandardColumn -> ColumnValue]
    case SuperColDef(value, sup) =>
      val resCol = result.getSuper_column
      var out = Map[StandardColumn[SuperColumn], ColumnValue[SuperColumn]]()
      new IteratorWrapper(resCol.getColumnsIterator).foreach { (column) =>
        val left  = sup \ column.getName
        val right = left.\(column.getValue, column.getTimestamp)
        out = out + (left -> right)
      }
      out.asInstanceOf[E]
  }


  /**
   *  takes any column name and provides a column path to that column. column
   * names will be either a standard column which belongs to a standard key,
   * a super column, or a standard column which belongs to a super key.
   */
  private def toColumnPath(col:ColumnName[_]):ColumnPath = columnToDefinition(col) match {
    case StandardColDef(value, std) =>
      val out = new ColumnPath(std.family.value)
      out.setColumn(value)
    case SuperColDef(value, sup) =>
      val out = new ColumnPath(sup.family.value)
      out.setColumn(value)
    case SuperStandardColDef(SuperColDef(superVal, _), StandardColDef(colVal, std)) =>
      val out = new ColumnPath(std.family.value)
      out.setColumn(colVal)
      out.setSuper_column(superVal)
  }


  /**
   * maps a column name to a definition which should allow for better pattern
   * matching.
   */
  private def columnToDefinition(col:ColumnName[_]) = col.key match {
    case key:StandardKey => StandardColDef(col.value, col.asInstanceOf[StandardColumn[_]])
    case key:SuperKey    => col match {
      case sc:SuperColumn                 => SuperColDef(col.value, sc)
      case sc:StandardColumn[SuperColumn] => SuperStandardColDef(SuperColDef(sc.owner.value, sc.owner), StandardColDef(sc.value, sc))
    }
  }

  private abstract class ColumnDefinition
  private case class StandardColDef(val value:Array[Byte], val column:StandardColumn[_]) extends ColumnDefinition
  private case class SuperColDef(val value:Array[Byte], val column:SuperColumn) extends ColumnDefinition
  private case class SuperStandardColDef(val superCol:SuperColDef, val standardCol:StandardColDef) extends ColumnDefinition

  private class IteratorWrapper[A](iter:java.util.Iterator[A]) {
    def foreach(f: A => Unit): Unit = {
      while(iter.hasNext) f(iter.next)
    }
  }

}