package com.shorrockin.cascal


import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import collection.jcl.Conversions._
import java.util.Arrays
import java.util.{List => JList}
import com.shorrockin.cascal.Conversions._

import model._
import collection.jcl.Buffer
import org.apache.cassandra.thrift.Cassandra
import org.apache.cassandra.thrift.{NotFoundException, ConsistencyLevel}

/**
 * a cascal session is the entry point for interacting with the
 * cassandra system through various path elements.
 *
 * @author Chris Shorrock
 */
class Session(val host:String, val port:Int, val defaultConsistency:Consistency) {

  private val sock    = new TSocket(host, port);
  private val tr      = new TBinaryProtocol(sock);

  val client  = new Cassandra.Client(tr,tr);
  sock.open();


  /**
   * closes this session
   */
  def close() = sock.close();

  
  /**
   * return the current cluster name of the cassandra instance
   */
  lazy val clusterName = client.get_string_property("cluster name")


  /**
   * returns the configuration file of the connected cassandra instance
   */
  lazy val configFile = client.get_string_property("config file")


  /**
   * returns the version of the cassandra instance
   */
  lazy val version = client.get_string_property("version")


  /**
   * returns a map of tokens from the cassandra instance.
   */
  lazy val tokenMap = Map("1" -> "2", "3" -> "4") // in the form of {"token1":"host1","token2":"host2"}


  /**
   * returns all the keyspaces from the cassandra instance
   */
  lazy val keyspaces:Seq[String] = Buffer(client.get_string_list_property("keyspaces"))


  /**
   *  returns the column value for the specified column
   */
  def get[ResultType](col:Gettable[ResultType], consistency:Consistency):Option[ResultType] = {
    try {
      val result = client.get(col.keyspace.value, col.key.value, col.columnPath, consistency)
      Some(col.convertGetResult(result))
    } catch {
      case nfe:NotFoundException => None
    }
  }

  
  /**
   * returns the column value for the specified column, using the default consistency
   */
  def get[ResultType](col:Gettable[ResultType]):Option[ResultType] = get(col, defaultConsistency)


  /**
   * inserts the specified column value
   */
  def insert[E](col:Column[E], consistency:Consistency) = {
    client.insert(col.keyspace.value, col.key.value, col.columnPath, col.value, col.time, consistency)
    col
  }


  /**
   * inserts the specified column value using the default consistency
   */
  def insert[E](col:Column[E]):Column[E] = insert(col, defaultConsistency)


  /**
   * inserts a batch of columns all at once, this only incures one call

  def insert(cols:Seq[Column[_]], consistency:Consistency) = {
  } */


  /**
   *  counts the number of columns in the specified column container
   */
  def count(container:ColumnContainer[_ ,_], consistency:Consistency):Int = {
    client.get_count(container.keyspace.value, container.key.value, container.columnParent, consistency)
  }


  /**
   * performs count on the specified column container
   */
  def count(container:ColumnContainer[_, _]):Int = count(container, defaultConsistency)


  /**
   * removes the specified column container
   */
  def remove(container:ColumnContainer[_, _], consistency:Consistency):Unit = {
    client.remove(container.keyspace.value, container.key.value, container.columnPath, now, consistency)
  }


  /**
   * removes the specified column container using the default consistency
   */
  def remove(container:ColumnContainer[_, _]):Unit = remove(container, defaultConsistency)


  /**
   * removes the specified column container
   */
  def remove(column:Column[_], consistency:Consistency):Unit = {
    client.remove(column.keyspace.value, column.key.value, column.columnPath, now, consistency)
  }


  /**
   * removes the specified column container using the default consistency
   */
  def remove(column:Column[_]):Unit = remove(column, defaultConsistency)


  /**
   * performs a list of the provided standard key. uses the list of columns as the predicate
   * to determine which columns to return.
   */
  def list[ResultType](container:ColumnContainer[_, ResultType], predicate:Predicate, consistency:Consistency):ResultType = {
    val results = client.get_slice(container.keyspace.value, container.key.value, container.columnParent, predicate.slicePredicate, consistency)
    container.convertListResult(convertList(results))
  }


  /**
   * performs a list of the specified container using no predicate and the default consistency.
   */
  def list[ResultType](container:ColumnContainer[_, ResultType]):ResultType = list(container, EmptyPredicate, defaultConsistency)


  /**
   * performs a list of the specified container using the specified predicate value
   */
  def list[ResultType](container:ColumnContainer[_, ResultType], predicate:Predicate):ResultType = list(container, predicate, defaultConsistency)


  /**
   * given a list of keys, will perform a slice retrieving all the keys specified
   * applying the provided predicate against those keys. Assumes that all the keys
   * belong to the same keyspace and column family. If they are not, the first key
   * in the sequence what is used in this query.
   */
  def list[ColumnType, ListType](keys:Seq[Key[ColumnType, ListType]], predicate:Predicate, consistency:Consistency):Map[Key[ColumnType, ListType], ListType] = {
    if (keys.size > 0) {
      val firstKey   = keys(0)
      val keyspace   = firstKey.keyspace
      val keyStrings = keys.map { _.value }
      val results    = client.multiget_slice(keyspace.value, keyStrings, firstKey.columnParent, predicate.slicePredicate, consistency)

      def locate(str:String) = (keys.find { _.value.equals(str) }).get
      var out = Map[Key[ColumnType, ListType], ListType]()
      results.foreach { (tuple) =>
        val key   = locate(tuple._1)
        val value = key.convertListResult(tuple._2)
        out = out + (key -> value)
      }

      out
    } else {
      throw new IllegalArgumentException("must provide at least 1 key for a list(keys, predicate, consistency) call")
    }
  }


  /**
   * @see list(Seq[Key], Predicate, Consistency)
   */
  def list[ColumnType, ListType](keys:Seq[Key[ColumnType, ListType]]):Map[Key[ColumnType, ListType], ListType] = list(keys, EmptyPredicate, defaultConsistency)


  /**
   * @see list(Seq[Key], Predicate, Consistency)
   */
  def list[ColumnType, ListType](keys:Seq[Key[ColumnType, ListType]], predicate:Predicate):Map[Key[ColumnType, ListType], ListType] = list(keys, predicate, defaultConsistency)


  /**
   * implicitly coverts a consistency value to an int
   */
  private implicit def toThriftConsistency(c:Consistency):ConsistencyLevel = c.thriftValue


  /**
   * converts a column container to a parent structure

  private def toColumnParent(col:ColumnContainer[_, _]):ColumnParent = col match {
    case key:SuperColumn => val out = new ColumnParent(col.family.value) ; out.setSuper_column(key.value)
    case key:Key[_ , _]  => new ColumnParent(col.family.value)
  }
  */


  /**
   * retuns the current time in milliseconds
   */
  private def now = System.currentTimeMillis


  /**
   * converts the specified key to a column path

  private def toColumnPath(c:ColumnContainer[_, _]):ColumnPath = c match {
    case key:SuperColumn => val out = new ColumnPath(c.family.value) ; out.setSuper_column(key.value)
    case key:Key[_ , _]  => new ColumnPath(c.family.value)
  }
*/


  /**
   * takes any column name and provides a column path to that column. column
   * names will be either a standard column which belongs to a standard key,
   * a super column, or a standard column which belongs to a super key.

  private def toColumnPath(get:Gettable[_]):ColumnPath = {
    def path(col:Array[Byte], sup:Array[Byte]) = {
      val out = new ColumnPath(get.family.value)
      out.setColumn(col)
      out.setSuper_column(sup)
      out
    }

    get match {
      case sc:SuperColumn => path(null, sc.value)
      case col:Column[_]  => col.owner match {
        case owner:SuperColumn => path(col.name, owner.value)
        case key:StandardKey   => path(col.name, null)
      }
    }
  }
   */
}