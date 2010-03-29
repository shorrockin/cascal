package com.shorrockin.cascal

import org.apache.cassandra.thrift.ConsistencyLevel

/**
 * object containing all the types of consistencies support by cassandra
 */
object Consistency {
  val Zero = new Consistency { def thriftValue = ConsistencyLevel.ZERO }
  val One = new Consistency { def thriftValue = ConsistencyLevel.ONE }
  val Quorum = new Consistency { def thriftValue = ConsistencyLevel.QUORUM }
  val DcQuorum = new Consistency { def thriftValue = ConsistencyLevel.DCQUORUM }
  def DcQuorumSync = new Consistency { def thriftValue = ConsistencyLevel.DCQUORUMSYNC }
  def All = new Consistency { def thriftValue = ConsistencyLevel.ALL }
}


/**
 * trait to define the various consistency levels supported by
 * cassandra.
 */
trait Consistency {
  def thriftValue:ConsistencyLevel
}