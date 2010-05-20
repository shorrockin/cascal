package com.shorrockin.cascal.session

import org.apache.commons.pool.PoolableObjectFactory
import org.apache.commons.pool.impl.{GenericObjectPoolFactory, GenericObjectPool}
import com.shorrockin.cascal.utils.Logging
import com.shorrockin.cascal.jmx.CascalStatistics
import com.shorrockin.cascal.model._


/**
 * a session pool which maintains a collection of open sessions so that
 * we can avoid the overhead of creating a new tcp connection every time
 * something is need.
 *
 * session pool is also an instance of a session template - when used in
 * this fashion each invocation to the sessiontemplate method will invoke
 * a borrow method and an execution of the requested method against the
 * session returned.
 *
 * @author Chris Shorrock
 */
class SessionPool(val hosts:Seq[Host], val params:PoolParams, consistency:Consistency, framedTransport:Boolean) extends SessionTemplate {
  def this(hosts:Seq[Host], params:PoolParams, consistency:Consistency) = this(hosts, params, consistency, false)
  def this(hosts:Seq[Host], params:PoolParams) = this(hosts, params, Consistency.One, false)

  CascalStatistics.register(this)

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
  def close() {
    pool.close
    CascalStatistics.unregister(this)
  }


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
  def borrow[E](f:(Session) => E):E = {
    var session:Session = null

    try {
      session = checkout
      val before = System.currentTimeMillis
      val out = f(session)
      CascalStatistics.usage(session.host, System.currentTimeMillis - before)
      out
    } catch {
      case t:Throwable => {
        if (null != session) CascalStatistics.usageError(session.host)
        throw t
      }
    } finally {
      if (null != session) checkin(session)
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
    // instead of randomly choosing a host we'll attempt to round-robin them, may not
    // be completely round robin with multiple threads but it should provide a more
    // even spread than something random.
    var lastHostUsed = 0

    def next(current:Int) = (current + 1) % hosts.size
    def makeObject:Object = makeSession(next(lastHostUsed), 0)

    def makeSession(hostIndex:Int, count:Int):Session = {
      if (count < hosts.size) {
        lastHostUsed = hostIndex
        val host = hosts(hostIndex)

        try {
          log.debug("attempting to create connection to: " + host)
          val session = new Session(host.address, host.port, host.timeout, consistency, framedTransport)
          session.open
          CascalStatistics.creation(host)
          session
        } catch {
          case e:Exception =>
            log.warn("encountered exception while creating connection(" + host + "), will attempt next host in configuration", e)
            CascalStatistics.creationError(host)
            makeSession(next(hostIndex), count + 1)
        }
      } else {
        throw new IllegalStateException("unable to connect to any of the hosts in the pool")
      }
    }

    def session(obj:Object) = obj.asInstanceOf[Session]

    def activateObject(obj:Object):Unit = {}

    def destroyObject(obj:Object):Unit = session(obj).close

    def validateObject(obj:Object):Boolean = session(obj).isOpen && !session(obj).hasError

    def passivateObject(obj:Object):Unit = {}
  }


  def clusterName:String = borrow { _.clusterName }

  def configFile:String = borrow { _.configFile }

  def version:String = borrow { _.version }

  def keyspaces:Seq[String] = borrow { _.keyspaces }

  def get[ResultType](col:Gettable[ResultType], consistency:Consistency):Option[ResultType] = borrow { _.get(col, consistency) }

  def get[ResultType](col:Gettable[ResultType]):Option[ResultType] = borrow { _.get(col) }

  def insert[E](col:Column[E], consistency:Consistency):Column[E] = borrow { _.insert(col, consistency) }

  def insert[E](col:Column[E]):Column[E] = borrow { _.insert(col) }

  def count(container:ColumnContainer[_ ,_], consistency:Consistency):Int = borrow { _.count(container, consistency) }

  def count(container:ColumnContainer[_, _]):Int = borrow { _.count(container) }

  def remove(container:ColumnContainer[_, _], consistency:Consistency):Unit = borrow { _.remove(container, consistency) }

  def remove(container:ColumnContainer[_, _]):Unit = borrow { _.remove(container) }

  def remove(column:Column[_], consistency:Consistency):Unit = borrow { _.remove(column, consistency) }

  def remove(column:Column[_]):Unit = borrow { _.remove(column) }

  def list[ResultType](container:ColumnContainer[_, ResultType], predicate:Predicate, consistency:Consistency):ResultType = borrow { _.list(container, predicate, consistency) }

  def list[ResultType](container:ColumnContainer[_, ResultType]):ResultType = borrow { _.list(container) }

  def list[ResultType](container:ColumnContainer[_, ResultType], predicate:Predicate):ResultType = borrow { _.list(container, predicate) }

  def list[ColumnType, ResultType](containers:Seq[ColumnContainer[ColumnType, ResultType]], predicate:Predicate, consistency:Consistency):Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = borrow { _.list(containers, predicate, consistency) }

  def list[ColumnType, ResultType](containers:Seq[ColumnContainer[ColumnType, ResultType]]):Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = borrow { _.list(containers) }

  def list[ColumnType, ResultType](containers:Seq[ColumnContainer[ColumnType, ResultType]], predicate:Predicate):Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = borrow { _.list(containers, predicate) }

  def list[ColumnType, ListType](family:ColumnFamily[Key[ColumnType, ListType]], range:KeyRange, predicate:Predicate, consistency:Consistency):Map[Key[ColumnType, ListType], ListType] = borrow { _.list(family, range, predicate, consistency) }

  def list[ColumnType, ListType](family:ColumnFamily[Key[ColumnType, ListType]], range:KeyRange, consistency:Consistency):Map[Key[ColumnType, ListType], ListType] = borrow { _.list(family, range, consistency) }

  def list[ColumnType, ListType](family:ColumnFamily[Key[ColumnType, ListType]], range:KeyRange):Map[Key[ColumnType, ListType], ListType] = borrow { _.list(family, range) }

  def batch(ops:Seq[Operation], consistency:Consistency):Unit = borrow { _.batch(ops, consistency) }

  def batch(ops:Seq[Operation]):Unit = borrow { _.batch(ops) }
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