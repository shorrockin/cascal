package com.shorrockin.cascal


import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import collection.jcl.Conversions._
import com.shorrockin.cascal.Conversions._

import model._
import collection.jcl.Buffer
import org.apache.cassandra.thrift.{Mutation, Cassandra, NotFoundException, ConsistencyLevel}
import java.util.{Map => JMap, List => JList, HashMap, ArrayList}

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
   *   counts the number of columns in the specified column container
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
   * performs a list on a key range in the specified column family. the predicate is applied
   * with the provided consistency guaranteed. the key range may be a range of keys or a range
   * of tokens. This list call is only available when using an order-preserving partition.
   */
  def list[ColumnType, ListType](family:ColumnFamily[Key[ColumnType, ListType]], range:KeyRange, predicate:Predicate, consistency:Consistency):Map[Key[ColumnType, ListType], ListType] = {
    val results = client.get_range_slices(family.keyspace.value, family.columnParent, predicate.slicePredicate, range.cassandraRange, consistency)
    var map     = Map[Key[ColumnType, ListType], ListType]()

    results.foreach { (keyslice) =>
      val key = (family \ keyslice.key)
      map = map + (key -> key.convertListResult(keyslice.columns))
    }
    map
  }

  
  /**
   * performs a key-range list without any predicate
   */
  def list[ColumnType, ListType](family:ColumnFamily[Key[ColumnType, ListType]], range:KeyRange, consistency:Consistency):Map[Key[ColumnType, ListType], ListType] = {
    list(family, range, EmptyPredicate, consistency)
  }


  /**
   * performs a key-range list without any predicate, and using the default consistency
   */
  def list[ColumnType, ListType](family:ColumnFamily[Key[ColumnType, ListType]], range:KeyRange):Map[Key[ColumnType, ListType], ListType] = {
    list(family, range, EmptyPredicate, defaultConsistency)
  }


  /**
   * performs the specified seq of operations in batch. assumes all operations belong
   * to the same keyspace. If they do not then the first keyspace in the first operation
   * is used.
   */
  def batch(ops:Seq[Operation], consistency:Consistency):Unit = {
    if (ops.size > 0) {
      val keyToFamilyMutations = new HashMap[String, JMap[String, JList[Mutation]]]()
      val keyspace = ops(0).keyspace

      def getOrElse[A, B](map:JMap[A, B], key:A, f: => B):B = {
        if (map.containsKey(key)) {
          map.get(key)
        } else {
          val newValue = f
          map.put(key, newValue)
          newValue
        }
      }

      ops.foreach { (op) =>
        val familyToMutations = getOrElse(keyToFamilyMutations, op.key.value, new HashMap[String, JList[Mutation]]())
        val mutationList      = getOrElse(familyToMutations, op.family.value, new ArrayList[Mutation]())
        mutationList.add(op.mutation)
      }

      // TODO need to do a super column flatten as we'll have multple
      // of the same super columns in this list
      client.batch_mutate(keyspace.value, keyToFamilyMutations, consistency)
    } else {
      throw new IllegalArgumentException("cannot perform batch operation on 0 length operation sequence")
    }
  }


  /**
   * performs the list of operations in batch using the default consistency
   */
  def batch(ops:Seq[Operation]):Unit = batch(ops, defaultConsistency)


  /**
   * implicitly coverts a consistency value to an int
   */
  private implicit def toThriftConsistency(c:Consistency):ConsistencyLevel = c.thriftValue


  /**
   * retuns the current time in milliseconds
   */
  private def now = System.currentTimeMillis

}