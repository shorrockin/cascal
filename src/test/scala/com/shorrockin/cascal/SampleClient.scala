package com.shorrockin.cascal

import model.{StringValue, ByteValue, ColumnValue}

object SampleClient {
  import Conversions._

  def log(str:String, args:String*) = System.out.println(str.format(args:_*))
  def now = System.currentTimeMillis

  val Presence = "NOS" \ "Presence"
  val Events = "NOS" \\ "Events"

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
    val key    = Presence \ "1"
    val value1 = Presence \ "1" \ "Moo" \ ("Presence Time: " + new java.util.Date().toString)
    val value2 = Presence \ "1" \ "Foo" \ "Value 2"
    val value3 = Presence \ "1" \ "Bar" \ "Value 3"

    session.insert(value1)
    session.insert(value2)
    session.insert(value3)

    val result   = session.get(value1.name)
//    val count    = session.count(key)

//    val all = session.list(key)
//    val columnFilter = session.list(key, column.value :: value3.name.value :: Nil)
//    val limitFilter  = session.list(key, None, None, Order.Ascending, Some(2))
//    val alphaFilter  = session.list(key, Some("Bar"), Some("Created"), Order.Ascending, None)

    log("result: %s", result)
//    log("[%s] standard result, count: %s, value: %s", now, count, result)
//    log("[%s] standard result list count, all: %s, columnFilter: %s, limitFilter: %s, alphaFilter: %s", now, all.size, columnFilter.size, limitFilter.size, alphaFilter.size)
//    log("[%s] standard result list all: %s", now, toString(all) )
//    log("[%s] standard result list column filtered: %s", now, toString(columnFilter))
//    log("[%s] standard result list limit(2) filtered: %s", now, toString(limitFilter))
//    log("[%s] standard result list alpha(Bar -> Created) filtered: %s", now, toString(alphaFilter))
    log("\n")
  }


  def testSuper(session:Session) = {
    log("[%s] super about to insert/query", now)
    val key         = Events \ "1"
    val superColumn = Events \ "1" \ UUID()
    val value1      = superColumn \ "Message" \ ("Event Time: " + new java.util.Date().toString)
    val value2      = superColumn \ "Other Message" \ "Another Message"

    session.insert(value1)
    session.insert(value2)

    var columnResult = session.get(value1.name)
    var superResult  = session.get(superColumn)
    
//    var count    = session.count(key)
//    var subCount = session.count(superColumn)
    log("result: %s", columnResult)
    log("result2: %s", (superResult.keySet.map { superResult(_) }).toSeq)
//    log("[%s] super result, key count: %s, super column count: %s, value: %s", now, count, subCount, string(result.value))
  }

  implicit def colSeqtoString(list:Seq[ColumnValue[_]]):String = (list.map { (cv) => "[%s -> %s]".format(cv.name, cv) }).mkString("", ",", "")
  implicit def intToString(i:Int):String = Integer.toString(i)
  implicit def longToString(l:Long):String = java.lang.Long.toString(l)
  implicit def byteValueToString(obj:ByteValue):String = string(obj.value)
  implicit def stringValueToString(obj:StringValue):String = obj.value

}