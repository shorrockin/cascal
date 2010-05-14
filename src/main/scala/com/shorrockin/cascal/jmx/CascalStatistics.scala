package com.shorrockin.cascal.jmx


import management.ManagementFactory
import javax.management.ObjectName
import com.shorrockin.cascal.session.SessionPool

/**
 * object used to capture various metrics related to cascal and expose them through
 * a jmx interface.
 *
 * @author Chris Shorrock
 */
object CascalStatistics extends CascalStatistics$MBean {
  ManagementFactory.getPlatformMBeanServer.registerMBean(this, new ObjectName("com.shorrockin.cascal:name=CascalStatistics"))

  private var pools = List[SessionPool]()
  private var creationCount = 0L
  private var checkoutCount = 0L
  private var checkinCount = 0L
  private var totalUsageTime = 0L

  def register(pool:SessionPool) = pools = pool :: pools
  def unregister(pool:SessionPool) = pools = pools - pool

  def creationInc = creationCount = creationCount + 1
  def checkoutInc = checkoutCount = checkoutCount + 1
  def checkinInc = checkinCount = checkinCount + 1
  def usageInc(duration:Long) = totalUsageTime = totalUsageTime + duration

  def getNumberOfActiveConnections():Int = pools.foldLeft(0) { _ + _.active }
  def getNumberOfIdleConnections():Int = pools.foldLeft(0) { _ + _.idle }
  def getNumberOfSessionsCreated():Long = creationCount
  def getNumberOfConnectionsCheckedOut():Long = checkoutCount
  def getNumberOfConnectionsCheckedIn():Long = checkinCount
  def getAverageConnectionUsageTime():Long = totalUsageTime / getNumberOfConnectionsCheckedOut
}

trait CascalStatistics$MBean {
  def getNumberOfActiveConnections():Int
  def getNumberOfIdleConnections():Int
  def getNumberOfSessionsCreated():Long
  def getNumberOfConnectionsCheckedOut():Long
  def getNumberOfConnectionsCheckedIn():Long
  def getAverageConnectionUsageTime():Long

}