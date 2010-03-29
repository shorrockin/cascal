package com.shorrockin.cascal

object SampleClient {
  import Conversions._
  import UUID._

  def log(str:String, args:Any*) = System.out.println(str.format(args:_*))
  def now = System.currentTimeMillis

  def main(args:Array[String]) {
    val session = new Session("shorrockin.com", 9160, Consistency.One)

    try {
      testStandard(session)
      testSuper(session)
    } finally {
      session.close()
    }
  }

  def testStandard(session:Session) = {
    log("[%s] standard about to insert/query", now)
    val key    = "NOS" \ "Presence" \ "1"
    val column = key \ "State"
    val value  = column \ ("Presence Time: " + new java.util.Date().toString)
    val value2 = key \ "Foo" \ "Value 2"
    val value3 = key \ "Bar" \ "Value 3"

    session.insert(value)
    session.insert(value2)
    session.insert(value3)

    val result   = session.get(column)
    val count    = session.count(key)
    val all      = session.list(key)
    val filtered = session.list(key, column.value :: value3.name.value :: Nil)
    log("[%s] standard result, count: %s, value: %s", now, count, string(result.value))
    log("[%s] standard result list count, all: %s, filtered: %s", now, all.size, filtered.size)
    log("[%s] standard result list all: %s", now, all.map { (cv) => "[%s -> %s]".format(string(cv.name.value), string(cv.value)) })
    log("[%s] standard result list filtered: %s", now, filtered.map { (cv) => "[%s -> %s]".format(string(cv.name.value), string(cv.value))})
    log("\n")
  }


  def testSuper(session:Session) = {
    log("[%s] super about to insert/query", now)
    val key         = "NOS" \\ "Events" \ "1"
    val superColumn = key \ UUID.uuid
    val column      = superColumn \ "Message"
    val value       = column \ ("Event Time: " + new java.util.Date().toString)

    session.insert(value)
    var result   = session.get(column)
    var count    = session.count(key)
    var subCount = session.count(superColumn)
    log("[%s] super result, key count: %s, super column count: %s, value: %s", now, count, subCount, string(result.value))
  }




}