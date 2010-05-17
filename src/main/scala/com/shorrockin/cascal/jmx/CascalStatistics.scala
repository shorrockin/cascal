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
  private val objectName  = new ObjectName("com.shorrockin.cascal:name=CascalStatistics")
  private val mbeanServer = ManagementFactory.getPlatformMBeanServer

  reinstallMBean()

  private var pools = List[SessionPool]()
  private var creationCount = 0L
  private var checkoutCount = 0L
  private var checkinCount = 0L
  private var totalUsageTime = 0L

  /**
   * normally this shouldn't be an issue, since this is an object. However if this library
   * is loaded twice by different class-loaders (for example) we could run into a scenario
   * where register throws an already registered exception. This scenario is likely to
   * occur in situations where we're running Cassandra/Cascal using something like SBT where
   * the JVM stays around between runs and each test is run in an isolated classloader. This
   * fix DOES NOT address the situation where cascal is used in two separate classloaders
   * concurrently - which would be a problem. (that is a TODO item).*
   */
  def reinstallMBean() {
    if (mbeanServer.isRegistered(objectName)) mbeanServer.unregisterMBean(objectName)
    mbeanServer.registerMBean(this, objectName)
  }

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