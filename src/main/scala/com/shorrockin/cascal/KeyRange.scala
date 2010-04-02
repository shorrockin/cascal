package com.shorrockin.cascal

import org.apache.cassandra.thrift.{KeyRange => CassKeyRange}

/**
 * a key range is used when you list by keys to specified the start and end
 * of the keys you wish to fetch.
 *
 * The values of start and end are both inclusive in this scenario.
 *
 * @author Chris Shorrock
 */
case class KeyRange(start:String, end:String, limit:Int) {
  lazy val cassandraRange = {
    val range = new CassKeyRange(limit)
    range.setStart_key(start)
    range.setEnd_key(end)
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
case class TokenRange(tokenStart:String, tokenEnd:String, tokenLimit:Int) extends KeyRange(tokenStart, tokenEnd, tokenLimit) {
  override lazy val cassandraRange = {
    val range = new CassKeyRange(limit)
    range.setStart_token(start)
    range.setEnd_token(end)
    range
  }
}