package com.shorrockin.cascal.jmx


import management.ManagementFactory
import javax.management.ObjectName
import com.shorrockin.cascal.session.{Session, Host, SessionPool}

/**
 * object used to capture various metrics related to cascal and expose them through
 * a jmx interface.
 *
 * @author Chris Shorrock
 */
object CascalStatistics extends CascalStatistics$MBean {
  private val objectName  = new ObjectName("com.shorrockin.cascal:name=Statistics")
  private val mbeanServer = ManagementFactory.getPlatformMBeanServer

  reinstallMBean()

  private var pools = List[SessionPool]()
  private var hosts = Map[Host, HostStatistics]()

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


  /**
   * retrieves the stats for the specified host, creating and registering them if they don't
   * exist.
   */
  private def get(host:Host) = {
    if (hosts.contains(host)) {
      hosts(host)
    } else {
      this.synchronized {
        if (hosts.contains(host)) {
          hosts(host)
        } else {
          val hostObjectName = new ObjectName("com.shorrockin.cascal:name=Statistics-%s-%s-%s".format(host.address, host.port, host.timeout))
          if (mbeanServer.isRegistered(hostObjectName)) mbeanServer.unregisterMBean(hostObjectName)

          val stats = new HostStatistics(host)
          hosts = hosts + (host -> stats)
          mbeanServer.registerMBean(stats, hostObjectName)
          stats
        }
      }
    }
  }

  def register(pool:SessionPool)   = pools = pool :: pools
  def unregister(pool:SessionPool) = pools = pools.filterNot(_ == pool)

  def creation(host:Host)             = get(host).creation
  def creationError(host:Host)        = get(host).creationError
  def usage(host:Host, duration:Long) = get(host).usage(duration)
  def usageError(host:Host)           = get(host).usageError

  def getNumberOfActiveConnections():Int      = pools.foldLeft(0) { _ + _.active }
  def getNumberOfIdleConnections():Int        = pools.foldLeft(0) { _ + _.idle }
  def getNumberOfConnectionsUsed():Long       = hosts.foldLeft(0L) { _ + _._2.getNumberOfConnectionsUsed }
  def getAverageConnectionUsageTime():Long    = getNumberOfConnectionsUsed() / getTotalUsageTime()
  def getTotalUsageTime():Long                = hosts.foldLeft(0L) { _ + _._2.getTotalUsageTime }
  def getNumberOfCreationFailures():Long      = hosts.foldLeft(0L) { _ + _._2.getNumberOfCreationFailures }
  def getNumberOfUsageExceptions():Long       = hosts.foldLeft(0L) { _ + _._2.getNumberOfUsageExceptions }
  def getNumberOfSessionsCreated():Long       = hosts.foldLeft(0L) { _ + _._2.getNumberOfSessionsCreated }
}

class HostStatistics(host:Host) extends HostStatisticsMBean {
  var usedCount    = 0L
  var usageTime    = 0L
  var usageErrors  = 0L
  var created      = 0L
  var createFails  = 0L

  def creation             = created = created + 1
  def creationError        = createFails = createFails + 1
  def usage(duration:Long) = {usedCount = usedCount + 1 ; usageTime = usageTime + duration }
  def usageError           = usageErrors = usageErrors + 1

  def getNumberOfConnectionsUsed()    = usedCount
  def getAverageConnectionUsageTime() = usedCount / usageTime
  def getTotalUsageTime()             = usageTime
  def getNumberOfCreationFailures()   = createFails
  def getNumberOfUsageExceptions()    = usageErrors
  def getNumberOfSessionsCreated()    = created
}

trait HostStatisticsMBean {
  def getNumberOfConnectionsUsed():Long
  def getAverageConnectionUsageTime():Long
  def getTotalUsageTime():Long
  def getNumberOfCreationFailures():Long
  def getNumberOfUsageExceptions():Long
  def getNumberOfSessionsCreated():Long
}

trait CascalStatistics$MBean extends HostStatisticsMBean {
  def getNumberOfActiveConnections():Int
  def getNumberOfIdleConnections():Int
}
