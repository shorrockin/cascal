package com.shorrockin.cascal.session

import com.shorrockin.cascal.model._

/**
 * session template describes the core function of a session. sub-classes of a template
 * may define their own sub-class which defines how the underlying session be retrieved,
 * be it through a thread local variable, some internal state or any other mechonism.
 *
 * @author Chris Shorrock
 */
trait SessionTemplate {

  /**
   * return the current cluster name of the cassandra instance
   */
  def clusterName:String


  /**
   * returns the version of the cassandra instance
   */
  def version:String


  /**
   * returns all the keyspaces from the cassandra instance
   */
  def keyspaces:Seq[String]


  /**
   *  returns the column value for the specified column
   */
  def get[ResultType](col:Gettable[ResultType], consistency:Consistency):Option[ResultType]


  /**
   * returns the column value for the specified column, using the default consistency
   */
  def get[ResultType](col:Gettable[ResultType]):Option[ResultType]


  /**
   * inserts the specified column value
   */
  def insert[E](col:Column[E], consistency:Consistency):Column[E]


  /**
   * inserts the specified column value using the default consistency
   */
  def insert[E](col:Column[E]):Column[E]


  /**
   *   counts the number of columns in the specified column container
   */
  def count(container:ColumnContainer[_ ,_], consistency:Consistency):Int


  /**
   * performs count on the specified column container
   */
  def count(container:ColumnContainer[_, _]):Int


  /**
   * removes the specified column container
   */
  def remove(container:ColumnContainer[_, _], consistency:Consistency):Unit


  /**
   * removes the specified column container using the default consistency
   */
  def remove(container:ColumnContainer[_, _]):Unit


  /**
   * removes the specified column container
   */
  def remove(column:Column[_], consistency:Consistency):Unit


  /**
   * removes the specified column container using the default consistency
   */
  def remove(column:Column[_]):Unit


  /**
   * performs a list of the provided standard key. uses the list of columns as the predicate
   * to determine which columns to return.
   */
  def list[ResultType](container:ColumnContainer[_, ResultType], predicate:Predicate, consistency:Consistency):ResultType


  /**
   * performs a list of the specified container using no predicate and the default consistency.
   */
  def list[ResultType](container:ColumnContainer[_, ResultType]):ResultType


  /**
   * performs a list of the specified container using the specified predicate value
   */
  def list[ResultType](container:ColumnContainer[_, ResultType], predicate:Predicate):ResultType


  /**
   * given a list of containers, will perform a slice retrieving all the columns specified
   * applying the provided predicate against those keys. Assumes that all the containers
   * generate the same columnParent. That is, if they are super columns, they all have the
   * same super column name (existing to separate key values), and regardless of column
   * container type - belong to the same column family. If they are not, the first key
   * in the sequence what is used in this query.<br>
   */
  def list[ColumnType, ResultType](containers:Seq[ColumnContainer[ColumnType, ResultType]], predicate:Predicate, consistency:Consistency):Seq[(ColumnContainer[ColumnType, ResultType], ResultType)]


  /**
   * @see list(Seq[ColumnContainer], Predicate, Consistency)
   */
  def list[ColumnType, ResultType](containers:Seq[ColumnContainer[ColumnType, ResultType]]):Seq[(ColumnContainer[ColumnType, ResultType], ResultType)]


  /**
   * @see list(Seq[ColumnContainer], Predicate, Consistency)
   */
  def list[ColumnType, ResultType](containers:Seq[ColumnContainer[ColumnType, ResultType]], predicate:Predicate):Seq[(ColumnContainer[ColumnType, ResultType], ResultType)]


  /**
   * performs a list on a key range in the specified column family. the predicate is applied
   * with the provided consistency guaranteed. the key range may be a range of keys or a range
   * of tokens. This list call is only available when using an order-preserving partition.
   */
  def list[ColumnType, ListType](family:ColumnFamily[Key[ColumnType, ListType]], range:KeyRange, predicate:Predicate, consistency:Consistency):Map[Key[ColumnType, ListType], ListType]


  /**
   * performs a key-range list without any predicate
   */
  def list[ColumnType, ListType](family:ColumnFamily[Key[ColumnType, ListType]], range:KeyRange, consistency:Consistency):Map[Key[ColumnType, ListType], ListType]


  /**
   * performs a key-range list without any predicate, and using the default consistency
   */
  def list[ColumnType, ListType](family:ColumnFamily[Key[ColumnType, ListType]], range:KeyRange):Map[Key[ColumnType, ListType], ListType]


  /**
   * performs the specified seq of operations in batch. assumes all operations belong
   * to the same keyspace. If they do not then the first keyspace in the first operation
   * is used.
   */
  def batch(ops:Seq[Operation], consistency:Consistency):Unit


  /**
   * performs the list of operations in batch using the default consistency
   */
  def batch(ops:Seq[Operation]):Unit
}
