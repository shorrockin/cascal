package com.shorrockin.cascal

import model._
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import collection.jcl.Conversions._
import java.util.Arrays
import org.apache.cassandra.service.{Cassandra, SliceRange, SlicePredicate, ColumnPath, ColumnParent}

/**
 * a cascal session is the entry point for interacting with the
 * cassandra system through various path elements.
 *
 * @author Chris Shorrock
 */
class Session(val host:String, val port:Int, val defaultConsistency:Consistency) {

  // TODO - replace with a better way to retreive the connection
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
   * returns a map of takens from the cassandra instance.
   */
  def tokenMap = Map("1" -> "2", "3" -> "4") // in the form of {"token1":"host1","token2":"host2"}


  /**
   * returns all the keyspaces from the cassandra instance
   */
  def keyspaces = client.get_string_list_property("keyspaces")


  /**
   *  returns the column value for the specified column
   */
  def get[E <: ColumnContainer](col:StandardColumn[E], consistency:Consistency):ColumnValue[E] = {
    val result = client.get(col.keyspace.value, col.key.value, columnToPath(col), consistency).getColumn
    new ColumnValue(col, result.value, result.timestamp)
  }


  /**
   * returns the column value for the specified column, using the default consistency
   */
  def get[E <: ColumnContainer](col:StandardColumn[E]):ColumnValue[E] = get(col, defaultConsistency)


  /**
   * inserts the specified column value
   */
  def insert(value:ColumnValue[_], consistency:Consistency) {
    client.insert(value.keyspace.value, value.key.value, columnToPath(value.name), value.value, value.time, consistency)
  }


  /**
   * inserts the specified column value using the default consistency
   */
  def insert(value:ColumnValue[_]):Unit = insert(value, defaultConsistency)

  
  /**
   * counts the number of columns in the specified column container 
   */
  def count(container:ColumnContainer, consistency:Consistency):Int = {
    client.get_count(container.keyspace.value, container.key.value, containerToParent(container), consistency)
  }


  /**
   * performs count on the specified column container
   */
  def count(container:ColumnContainer):Int = count(container, defaultConsistency)

  
  /**
   * performs a list of the provided standard key. uses the list of columns as the predicate
   * to determine which columns to return.
   */
  def list(key:StandardKey, includeColumns:Seq[Array[Byte]], consistency:Consistency):Seq[ColumnValue[StandardKey]] = {
    val results = client.get_slice(key.keyspace.value, key.key.value, containerToParent(key), includeColumns, consistency)
    val cache   = Map[Array[Byte], StandardColumn[StandardKey]]()

    def lookup(value:Array[Byte]):StandardColumn[StandardKey] = {
      if (cache.contains(value)) cache.get(value).get else (key \ value)
    }

    convertList(results).map { (cosc) =>
      val rawColumn = cosc.getColumn
      val column    = lookup(rawColumn.getName)
      column.\(rawColumn.getValue, rawColumn.getTimestamp)
    }
  }


  /**
   * performs a list of the provided standard key, uses the included list to determine the
   * the columns to return. uses the default consistency level.
   */
  def list(key:StandardKey, includeColumns:Seq[Array[Byte]]):Seq[ColumnValue[StandardKey]] = list(key, includeColumns, defaultConsistency)


  /**
   * lists all the columns with the specified consistency
   */
  def list(key:StandardKey, consistency:Consistency):Seq[ColumnValue[StandardKey]] = list(key, Nil, consistency)


  /**
   * lists all the columns with the default consistency
   */
  def list(key:StandardKey):Seq[ColumnValue[StandardKey]] = list(key, Nil, defaultConsistency)


  /**
   * continence way to throw an error with a tokenized string
   */
  private def argumentError(msg:String, args:Any*) = throw new IllegalArgumentException(msg.format(args:_*))


  /**
   * implicitly coverts a consistency value to an int
   */
  private implicit def consistencyToInt(c:Consistency):Int = c.intValue


  /**
   * converts a list of columns to a slice predicate.
   */
  private implicit def columnsToPredicate(list:Seq[Array[Byte]]):SlicePredicate = list.size match {
    case 0 => new SlicePredicate(null, new SliceRange(new Array[Byte](0), new Array[Byte](0), false, Integer.MAX_VALUE))
    case _ => new SlicePredicate(list, null)
  }


  /**
   * converts a scala sequence to a java list
   */
  private implicit def seqToJavaList[T](l: Seq[T]):java.util.List[T] = l.foldLeft(new java.util.ArrayList[T](l.size)){(al, e) => al.add(e); al}


  /**
   * converts a column container to a parent structure
   */
  private def containerToParent(col:ColumnContainer):ColumnParent = col match {
    case key:SuperColumn => new ColumnParent(col.family.value, key.value)
    case key:Key         => new ColumnParent(col.family.value, null)
  }

  /**
   * converts the specified standard column to a path based on if it
   * was from a standard key, or a super column.
   */
  private def columnToPath(col:StandardColumn[_]):ColumnPath = col.owner match {
    case key:StandardKey => new ColumnPath(col.family.value, null, col.value)
    case key:SuperColumn => new ColumnPath(col.family.value, key.value, col.value)
  }
}