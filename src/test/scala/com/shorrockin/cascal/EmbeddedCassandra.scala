package com.shorrockin.cascal

import org.apache.cassandra.thrift.CassandraDaemon
import org.apache.cassandra.config.DatabaseDescriptor
import java.io.File



/**
 * trait which mixes in the functionality necessary to embed
 * cassandra into a unit test
 */
trait EmbeddedCassandra extends Logging {
  import Utils._

  def withSession(f:(Session) => Unit) = {
    EmbeddedCassandra.init
    val session = new Session("localhost", 9160, Consistency.One)
    manage(session) { f(session) }
  }
}

/**
 * maintains the single instance of the cassandra server
 */
object EmbeddedCassandra extends Logging {
  import Utils._
  var initialized = false

  def init = synchronized {
    if (!initialized) {
      val homeDirectory = new File("cassandra.home.unit-tests")
      delete(homeDirectory)
      homeDirectory.mkdirs

      log.debug("creating cassandra instance at: " + homeDirectory.getCanonicalPath)
      log.debug("copying cassandra configuration files to root directory")
    
      val fileSep     = System.getProperty("file.separator")
      val storageFile = new File(homeDirectory, "storage-conf.xml")
      val logFile     = new File(homeDirectory, "log4j.properties")

      replace(copy(resource("/storage-conf.xml"), storageFile), ("%temp-dir%" -> (homeDirectory.getCanonicalPath + fileSep)))
      copy(resource("/log4j.properties"), logFile)

      System.setProperty("storage-config", homeDirectory.getCanonicalPath)

      log.debug("creating data file and log location directories")
      DatabaseDescriptor.getAllDataFileLocations.foreach { (file) => new File(file).mkdirs }
      new File(DatabaseDescriptor.getLogFileLocation).mkdirs

      val daemon = new CassandraDaemonThread
      daemon.start

      initialized = true
    }
  }

  private def resource(str:String) = classOf[EmbeddedCassandra].getResourceAsStream(str)
}

/**
 * daemon thread used to start and stop cassandra
 */
class CassandraDaemonThread extends Thread("CassandraDaemonThread") with Logging {
  private val daemon = new CassandraDaemon

  setDaemon(true)

  override def run:Unit = {
    log.debug("initializing cassandra daemon")
    daemon.init(new Array[String](0))
    log.debug("starting cassandra daemon")
    daemon.start
    Thread.sleep(1000)
  }

  def close():Unit = {
    log.debug("instructing cassandra deamon to shut down")
    daemon.stop
    log.debug("blocking on cassandra shutdown")
    this.join
    log.debug("cassandra shut down")
  }
}

