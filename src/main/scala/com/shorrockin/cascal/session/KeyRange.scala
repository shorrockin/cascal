package com.shorrockin.cascal.session

import org.apache.cassandra.thrift.{KeyRange => CassKeyRange}
import java.nio.charset.Charset

/**
 * a key range is used when you list by keys to specified the start and end
 * of the keys you wish to fetch.
 *
 * The values of start and end are both inclusive in this scenario.
 *
 * @author Chris Shorrock
 */
object KeyRange {
  val utf8 = Charset.forName("UTF-8")
}

trait CassandraKeyRange {
  lazy val cassandraRange:CassKeyRange = null
}

case class KeyRange(start:String, end:String, limit:Int) extends CassKeyRange {
  lazy val cassandraRange = {
    val range = new CassKeyRange(limit)
    range.setStart_key(KeyRange.utf8.encode(start))
    range.setEnd_key(KeyRange.utf8.encode(end))
    range
  }
}


/**
 * a key range is used when you list by keys to specified the start and end
 * base on the tokens.
 *
 * The values of start and end are both exclusive in this scenario.
 *
 * @author Chris Shorrock
 */
case class TokenRange(tokenStart:String, tokenEnd:String, tokenLimit:Int) extends CassandraKeyRange {
  override lazy val cassandraRange = {
    val range = new CassKeyRange(tokenLimit)
    range.setStart_token(tokenStart)
    range.setEnd_token(tokenEnd)
    range
  }
}
