package com.shorrockin.cascal

import org.apache.cassandra.service.ConsistencyLevel

/**
 * object containing all the types of consistencies support by cassandra
 */
object Consistency {
  val Zero = new Consistency { def intValue = ConsistencyLevel.ZERO }
  val One = new Consistency { def intValue = ConsistencyLevel.ONE }
  val Quorum = new Consistency { def intValue = ConsistencyLevel.QUORUM }
  val DcQuorum = new Consistency { def intValue = ConsistencyLevel.DCQUORUM }
  def DcQuorumSync = new Consistency { def intValue = ConsistencyLevel.DCQUORUMSYNC }
  def All = new Consistency { def intValue = ConsistencyLevel.ALL }
}


/**
 * trait to define the various consistency levels supported by
 * cassandra.
 */
trait Consistency {
  def intValue:Int
}