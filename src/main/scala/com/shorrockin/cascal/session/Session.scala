package com.shorrockin.cascal.session

import org.apache.thrift.protocol.TBinaryProtocol

import collection.jcl.Buffer
import org.apache.cassandra.thrift.{Mutation, Cassandra, NotFoundException, ConsistencyLevel}
import java.util.{Map => JMap, List => JList, HashMap, ArrayList}



import collection.jcl.Conversions._
import com.shorrockin.cascal.utils.Conversions._

import com.shorrockin.cascal.model._
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import collection.mutable.HashSet

/**
 * a cascal session is the entry point for interacting with the
 * cassandra system through various path elements.
 *
 * @author Chris Shorrock
 */
class Session(val host: Host, val defaultConsistency: Consistency, val framedTransport: Boolean) extends SessionTemplate {
  def this(host: String, port: Int, timeout: Int, defaultConsistency: Consistency, framedTransport: Boolean) = this (Host(host, port, timeout), defaultConsistency, framedTransport)

  def this(host: String, port: Int, timeout: Int, defaultConsistency: Consistency) = this (host, port, timeout, defaultConsistency, false)

  def this(host: String, port: Int, timeout: Int) = this (host, port, timeout, Consistency.One, false)

  private val sock = {
    if (framedTransport) new TFramedTransport(new TSocket(host.address, host.port, host.timeout))
    else new TSocket(host.address, host.port, host.timeout)
  }

  private val protocol = new TBinaryProtocol(sock)

  var lastError: Option[Throwable] = None

  val client = new Cassandra.Client(protocol, protocol)

  /**
   * opens the socket
   */
  def open() = sock.open()


  /**
   * closes this session
   */
  def close() = {
    sock.close()
    protocol.getTransport.close()
  }


  /**
   * returns true if this session is open
   */
  def isOpen = sock.isOpen


  /**
   * true if the we experienced an error while using this session
   */
  def hasError = lastError.isDefined


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
   * returns all the keyspaces from the cassandra instance
   */
  lazy val keyspaces: Seq[String] = Buffer(client.get_string_list_property("keyspaces"))

  /**
   * returns the
   */

  lazy val keyspaceDescriptors: HashSet[Tuple3[String, String, String]] = {
    var keyspaceDesc = new HashSet[Tuple3[String, String, String]]
    client.describe_keyspaces foreach {
      space =>
        val familyMap = client.describe_keyspace(space)
        familyMap.keySet foreach {
          family =>
            keyspaceDesc.addEntry((space, family, familyMap.get(family).get("Type")))
            ()
        }
    }
    keyspaceDesc
  }

  def verifyInsert[E](col: Column[E]) {
    var famType = if (col.owner.isInstanceOf[SuperColumn]) "Super" else "Standard"
    if (!keyspaceDescriptors.contains(col.keyspace.value, col.family.value, famType)) {
      throw new IllegalArgumentException("Keyspace %s or ColumnFamily %s of type %s does not exist in this cassandra instance".format(col.keyspace.value, col.family.value, famType))
    }
  }

  def verifyRemove(container: ColumnContainer[_, _]) {
    if (!keyspaceDescriptors.contains(container.keyspace.value, container.family.value, "Standard") &&
            !keyspaceDescriptors.contains(container.keyspace.value, container.family.value, "Super"))
      throw new IllegalArgumentException("Keyspace %s or ColumnFamily %s does not exist in this cassandra instance".format(container.keyspace.value, container.family.value))
  }

  def verifyOperation(op: Operation) {
    if (op.isInstanceOf[Insert]) {
      verifyInsert(op.asInstanceOf[Insert].column)
    } else if (op.isInstanceOf[Delete]) {
      verifyRemove(op.asInstanceOf[Delete].container)
    }
  }

  /**
   *  returns the column value for the specified column
   */
  def get[ResultType](col: Gettable[ResultType], consistency: Consistency): Option[ResultType] = detect {
    try {
      val result = client.get(col.keyspace.value, col.key.value, col.columnPath, consistency)
      Some(col.convertGetResult(result))
    } catch {
      case nfe: NotFoundException => None
    }
  }


  /**
   * returns the column value for the specified column, using the default consistency
   */
  def get[ResultType](col: Gettable[ResultType]): Option[ResultType] = get(col, defaultConsistency)


  /**
   * inserts the specified column value
   */
  def insert[E](col: Column[E], consistency: Consistency) = detect {
    verifyInsert(col)
    client.insert(col.keyspace.value, col.key.value, col.columnPath, col.value, col.time, consistency)
    col
  }


  /**
   * inserts the specified column value using the default consistency
   */
  def insert[E](col: Column[E]): Column[E] = insert(col, defaultConsistency)


  /**
   *   counts the number of columns in the specified column container
   */
  def count(container: ColumnContainer[_, _], consistency: Consistency): Int = detect {
    client.get_count(container.keyspace.value, container.key.value, container.columnParent, consistency)
  }


  /**
   * performs count on the specified column container
   */
  def count(container: ColumnContainer[_, _]): Int = count(container, defaultConsistency)


  /**
   * removes the specified column container
   */
  def remove(container: ColumnContainer[_, _], consistency: Consistency): Unit = detect {
    verifyRemove(container)
    client.remove(container.keyspace.value, container.key.value, container.columnPath, now, consistency)
  }


  /**
   * removes the specified column container using the default consistency
   */
  def remove(container: ColumnContainer[_, _]): Unit = remove(container, defaultConsistency)


  /**
   * removes the specified column container
   */
  def remove(column: Column[_], consistency: Consistency): Unit = detect {
    client.remove(column.keyspace.value, column.key.value, column.columnPath, now, consistency)
  }


  /**
   * removes the specified column container using the default consistency
   */
  def remove(column: Column[_]): Unit = remove(column, defaultConsistency)


  /**
   * performs a list of the provided standard key. uses the list of columns as the predicate
   * to determine which columns to return.
   */
  def list[ResultType](container: ColumnContainer[_, ResultType], predicate: Predicate, consistency: Consistency): ResultType = detect {
    val results = client.get_slice(container.keyspace.value, container.key.value, container.columnParent, predicate.slicePredicate, consistency)
    container.convertListResult(convertList(results))
  }


  /**
   * performs a list of the specified container using no predicate and the default consistency.
   */
  def list[ResultType](container: ColumnContainer[_, ResultType]): ResultType = list(container, EmptyPredicate, defaultConsistency)


  /**
   * performs a list of the specified container using the specified predicate value
   */
  def list[ResultType](container: ColumnContainer[_, ResultType], predicate: Predicate): ResultType = list(container, predicate, defaultConsistency)


  /**
   * given a list of containers, will perform a slice retrieving all the columns specified
   * applying the provided predicate against those keys. Assumes that all the containers
   * generate the same columnParent. That is, if they are super columns, they all have the
   * same super column name (existing to separate key values), and regardless of column
   * container type - belong to the same column family. If they are not, the first key
   * in the sequence what is used in this query.<br>
   * NOTE (to be clear): If containers is a 
   */
  def list[ColumnType, ResultType](containers: Seq[ColumnContainer[ColumnType, ResultType]], predicate: Predicate, consistency: Consistency): Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = {
    if (containers.size > 0) detect {
      val firstContainer = containers(0)
      val keyspace = firstContainer.keyspace
      val keyStrings = containers.map {_.key.value}
      val results = client.multiget_slice(keyspace.value, keyStrings, firstContainer.columnParent, predicate.slicePredicate, consistency)

      def locate(key: String) = (containers.find {_.key.value.equals(key)}).get

      results.map {
        (tuple) =>
          val key = locate(tuple._1)
          val value = key.convertListResult(tuple._2)
          (key -> value)
      }.toSeq
    } else {
      throw new IllegalArgumentException("must provide at least 1 container for a list(keys, predicate, consistency) call")
    }
  }


  /**
   * @see list ( Seq[ColumnContainer], Predicate, Consistency )
   */
  def list[ColumnType, ResultType](containers: Seq[ColumnContainer[ColumnType, ResultType]]): Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = list(containers, EmptyPredicate, defaultConsistency)


  /**
   * @see list ( Seq[ColumnContainer], Predicate, Consistency )
   */
  def list[ColumnType, ResultType](containers: Seq[ColumnContainer[ColumnType, ResultType]], predicate: Predicate): Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = list(containers, predicate, defaultConsistency)


  /**
   * performs a list on a key range in the specified column family. the predicate is applied
   * with the provided consistency guaranteed. the key range may be a range of keys or a range
   * of tokens. This list call is only available when using an order-preserving partition.
   */
  def list[ColumnType, ListType](family: ColumnFamily[Key[ColumnType, ListType]], range: KeyRange, predicate: Predicate, consistency: Consistency): Map[Key[ColumnType, ListType], ListType] = detect {
    val results = client.get_range_slices(family.keyspace.value, family.columnParent, predicate.slicePredicate, range.cassandraRange, consistency)
    var map = Map[Key[ColumnType, ListType], ListType]()

    results.foreach {
      (keyslice) =>
        val key = (family \ keyslice.key)
        map = map + (key -> key.convertListResult(keyslice.columns))
    }
    map
  }


  /**
   * performs a key-range list without any predicate
   */
  def list[ColumnType, ListType](family: ColumnFamily[Key[ColumnType, ListType]], range: KeyRange, consistency: Consistency): Map[Key[ColumnType, ListType], ListType] = {
    list(family, range, EmptyPredicate, consistency)
  }


  /**
   * performs a key-range list without any predicate, and using the default consistency
   */
  def list[ColumnType, ListType](family: ColumnFamily[Key[ColumnType, ListType]], range: KeyRange): Map[Key[ColumnType, ListType], ListType] = {
    list(family, range, EmptyPredicate, defaultConsistency)
  }


  /**
   * performs the specified seq of operations in batch. assumes all operations belong
   * to the same keyspace. If they do not then the first keyspace in the first operation
   * is used.
   */
  def batch(ops: Seq[Operation], consistency: Consistency): Unit = {
    if (ops.size > 0) detect {
      val keyToFamilyMutations = new HashMap[String, JMap[String, JList[Mutation]]]()
      val keyspace = ops(0).keyspace

      def getOrElse[A, B](map: JMap[A, B], key: A, f: => B): B = {
        if (map.containsKey(key)) {
          map.get(key)
        } else {
          val newValue = f
          map.put(key, newValue)
          newValue
        }
      }

      ops.foreach {
        (op) =>
          verifyOperation(op)
          val familyToMutations = getOrElse(keyToFamilyMutations, op.key.value, new HashMap[String, JList[Mutation]]())
          val mutationList = getOrElse(familyToMutations, op.family.value, new ArrayList[Mutation]())
          mutationList.add(op.mutation)
      }

      // TODO may need to flatten duplicate super columns?
      client.batch_mutate(keyspace.value, keyToFamilyMutations, consistency)
    } else {
      throw new IllegalArgumentException("cannot perform batch operation on 0 length operation sequence")
    }
  }


  /**
   * performs the list of operations in batch using the default consistency
   */
  def batch(ops: Seq[Operation]): Unit = batch(ops, defaultConsistency)


  /**
   * implicitly coverts a consistency value to an int
   */
  private implicit def toThriftConsistency(c: Consistency): ConsistencyLevel = c.thriftValue


  /**
   * retuns the current time in milliseconds
   */
  private def now = System.currentTimeMillis


  /**
   * all calls which access the session should be wrapped within this method,
   * it will catch any exceptions and make sure the session is then removed
   * from the pool.
   */
  private def detect[T](f: => T) = try {
    f
  } catch {
    case t: Throwable => lastError = Some(t); throw t
  }

}