package com.shorrockin.cascal.session

import org.apache.commons.pool.PoolableObjectFactory
import org.apache.commons.pool.impl.{GenericObjectPoolFactory, GenericObjectPool}
import com.shorrockin.cascal.utils.Logging

/**
 * a session pool which maintains a collection of open sessions so that
 * we can avoid the overhead of creating a new tcp connection every time
 * something is need.
 *
 * @author Chris Shorrock
 */
class SessionPool(val hosts:Seq[Host], val params:PoolParams, consistency:Consistency) {

  private val pool = {
    val factory = new GenericObjectPoolFactory(SessionFactory,
                                               params.maxActive,
                                               params.exhaustionPolicy.value,
                                               params.maxWait,
                                               params.maxIdle,
                                               params.minIdle,
                                               params.testOnBorrow,
                                               params.testOnReturn,
                                               params.timeBetweenEvictionsRunsMillis,
                                               params.numTestsPerEvictionRuns,
                                               params.minEvictableIdleTimeMillis,
                                               params.testWhileIdle,
                                               params.softMinEvictableIdleTimeMillis,
                                               params.lifo)
    factory.createPool
  }


  /**
   * closes this pool and releases any resources available to it.
   */
  def close() { pool.close }


  /**
   * clears any idle objects sitting in the pool (optional operation)
   */
  def clear() { pool.clear }


  /**
   * returns the number of active session connections. 
   */
  def active = pool.getNumActive


  /**
   * returns the number of idle session connections
   */
  def idle = pool.getNumIdle


  /**
   * used to retrieve a session and perform a function using that
   * function. This function will clean up the borrowed object after
   * it has finished. You do not need to manually call "return"
   */
  def borrow[E](f:(Session) => E) {
    var session:Session = null

    try {
      session = pool.borrowObject.asInstanceOf[Session]
      f(session)
    } finally {
      if (null != session) pool.returnObject(session)
    }
  }


  /**
   * retrieves a session. Once the caller has finished with the
   * session it must be returned to the pool. failure to do so
   * will result your pool shedding a tear. 
   */
  def checkout:Session = pool.borrowObject.asInstanceOf[Session]


  /**
   * returns the session back to the pool. only necessary when a sessio
   * is retrieved through the checkout methad.
   */
  def checkin(session:Session) = pool.returnObject(session)


  /**
   * used to create sessions
   */
  private object SessionFactory extends PoolableObjectFactory with Logging {
    val random = new java.util.Random

    def makeObject:Object = makeSession(random.nextInt(hosts.size), 0)

    def makeSession(hostIndex:Int, count:Int):Session = {
      if (count < hosts.size) {
        val host = hosts(hostIndex)

        try {
          log.debug("attempting to create connection to: " + host)
          val session = new Session(host.address, host.port, host.timeout, consistency)
          session.open
          session
        } catch {
          case e:Exception =>
            log.warn("encountered exception while creating connection(" + host + "), will attempt next host in configuration", e)
            makeSession((hostIndex + 1) % hosts.size, count + 1)
        }
      } else {
        // TODO throw something of more substance
        throw new IllegalStateException("unable to connect to any of the hosts in the pool")
      }
    }

    def session(obj:Object) = obj.asInstanceOf[Session]

    def activateObject(obj:Object):Unit = {}

    def destroyObject(obj:Object):Unit = session(obj).close

    def validateObject(obj:Object):Boolean = session(obj).isOpen

    def passivateObject(obj:Object):Unit = {}
  }
}

/**
 * case class used when configuring the session pool
 */
case class Host(address:String, port:Int, timeout:Int) {
  def ::(other:Host) = other :: this :: Nil
}



/**
 * defines an exhaustion policy used by the cassandra system.
 */
trait ExhaustionPolicy {
  def value:Byte
}


/**
 * defines the possible values for the exhaustion policy.
 */
object ExhaustionPolicy {
  val Fail = new ExhaustionPolicy { val value = GenericObjectPool.WHEN_EXHAUSTED_FAIL }
  val Grow = new ExhaustionPolicy { val value = GenericObjectPool.WHEN_EXHAUSTED_GROW }
  val Block = new ExhaustionPolicy { val value = GenericObjectPool.WHEN_EXHAUSTED_BLOCK }
}


/**
 * this class tempts me to upgrade to 2.8. until then it describes, in the
 * most verbose fashion possible, all the parameters that can be passed into
 * the session pool.
 *
 * @author Chris Shorrock
 */
case class PoolParams(maxActive:Int,
                      exhaustionPolicy:ExhaustionPolicy,
                      maxWait:Long,
                      maxIdle:Int,
                      minIdle:Int,
                      testOnBorrow:Boolean,
                      testOnReturn:Boolean,
                      timeBetweenEvictionsRunsMillis:Long,
                      numTestsPerEvictionRuns:Int,
                      minEvictableIdleTimeMillis:Long,
                      testWhileIdle:Boolean,
                      softMinEvictableIdleTimeMillis:Long,
                      lifo:Boolean) {
  def this(maxActive:Int,
           exhaustionPolicy:ExhaustionPolicy,
           maxWait:Long,
           maxIdle:Int,
           minIdle:Int) = this(maxActive,
                               exhaustionPolicy,
                               maxWait,
                               maxIdle,
                               minIdle,
                               true,
                               true,
                               GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS,
                               GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN,
                               GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS ,
                               GenericObjectPool.DEFAULT_TEST_WHILE_IDLE,
                               GenericObjectPool.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS,
                               true)
}