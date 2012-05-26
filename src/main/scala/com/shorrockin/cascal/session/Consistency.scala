package com.shorrockin.cascal.session

import org.apache.cassandra.thrift.ConsistencyLevel

/**
 * object containing all the types of consistencies support by cassandra
 *
 * @author Chris Shorrock
 */
object Consistency {
  /**
   * WRITE: Ensure that the write has been written to at least 1 node,
   * including hinted recipients.
   *
   * READ: Not supported. You probably want ONE instead
   */
  val Any = new Consistency { def thriftValue = ConsistencyLevel.ANY }

  /**
   * WRITE: Ensure that the write has been written to at least 1 node's
   * commit log and memory table before responding to the client.
   *
   * READ: Will return the record returned by the first node to respond.
   * A consistency check is always done in a background thread to fix any
   * consistency issues when One is used. This means subsequent
   * calls will have correct data even if the initial read gets an older
   * value. (This is called read repair.)
   */
  val One = new Consistency { def thriftValue = ConsistencyLevel.ONE }

  /**
   * WRITE: Ensure that the write has been written to <ReplicationFactor> / 2 + 1
   * nodes before responding to the client.
   *
   * READ: Will query all nodes and return the record with the most recent
   * timestamp once it has at least a majority of replicas reported. Again,
   * the remaining replicas will be checked in the background.
   */
  val Quorum = new Consistency { def thriftValue = ConsistencyLevel.QUORUM }

  /**
   * WRITE: Ensure that the write is written to all <ReplicationFactor> nodes
   * before responding to the client. Any unresponsive nodes will fail the operation.
   *
   * READ: Will query all nodes and return the record with the most recent
   * timestamp once all nodes have replied. Any unresponsive nodes will fail
   * the operation.
   */
  def All = new Consistency { def thriftValue = ConsistencyLevel.ALL }
}


/**
 * trait to define the various consistency levels supported by
 * cassandra.
 *
 * @author Chris Shorrock
 */
trait Consistency {
  def thriftValue:ConsistencyLevel
}
