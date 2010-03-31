package com.shorrockin.cascal

import java.util.Date
import model.{Column, StringValue, ByteValue}

object SampleClient {
  import Conversions._
  import collection.jcl.Conversions._

  def log(str:String, args:String*) = System.out.println(str.format(args:_*))
  def now = System.currentTimeMillis

  val Presence = "NOS" \ "Presence"
  val Events   = "NOS" \\ "Events"

  def main(args:Array[String]) {
    val session = new Session("shorrockin.com", 9160, Consistency.One)

    try {
      log("[%s] Cluster Name: %s", now, session.clusterName)
      log("[%s] Version: %s", now, session.version)
      log("[%s] Keyspaces: %s", now, session.keyspaces.mkString("", ",", ""))
      // log("[%s] Config File: %s", now, session.configFile)
      log("---------------------------------------------------")
      testStandard(session)
      log("---------------------------------------------------")
      testSuper(session)
    } finally {
      session.close()
    }
  }

  def testStandard(session:Session) = {
    val key  = Presence \ "1"
    val moo  = Presence \ "1" \ ("Moo", "Moo Val: " + new Date())
    val foo  = Presence \ "1" \ ("Foo", "Foo Val: " + new Date())
    val bar  = Presence \ "1" \ ("Bar", "Bar Val: " + new Date())

    log("[%s] inserting 'Moo', 'Foo', and 'Bar'", now)
    session.insert(moo)
    session.insert(foo)
    session.insert(bar)

    log("[%s] get('Moo'): %s", now, session.get(moo))
    log("[%s] get('Foo'): %s", now, session.get(foo))
    log("[%s] count(1): %s", now, session.count(key))

    session.list(key).foreach { (column) =>
      log("[%s] list(key '1'): %s", now, column)
    }

    session.list(key, RangePredicate("Bar", "Foo")).foreach { (column) =>
      log("[%s] list(key '1', 'Bar' -> 'Foo'): %s", now, column)
    }

    session.list(key, ColumnPredicate(List("Moo", "Bar"))).foreach { (column) =>
      log("[%s] list(key 1, 'Moo' & 'Bar'): %s", now, column)
    }
  }


  def testSuper(session:Session) = {
    log("[%s] super about to insert/query", now)
    val timeUUID    = UUID()
    val key         = Events \ "1"
    val superColumn = Events \ "1" \ timeUUID
    val value1      = Events \ "1" \ timeUUID \ "Message" \ ("Event Time: " + new Date())
    val value2      = Events \ "1" \ timeUUID \ "Other Message" \ "Another Message"

    session.insert(value1)
    session.insert(value2)

    val columnResult = session.get(value1)
    val superResult  = session.get(superColumn)
    
    log("[%s] standard column result: %s", now, columnResult)
    log("[%s] super column get result for: %s", now, UUID(superColumn.value))
    superResult.foreach { (column) => log("[%s]    %s", now, column) }
    
    log("[%s] number of super columns: %s, number of columns in first super: %s", now, session.count(key), session.count(superColumn))

    // example listing at the super key level
    val superKeyMap = session.list(key, RangePredicate(Order.Descending, 5))
    log("[%s] super column list return %s keys", now, superKeyMap.size)
    superKeyMap.foreach { (sc) =>
      log("[%s]  SuperKey: %s", now, UUID(sc._1.value))
      superKeyMap(sc._1).foreach { (column) => log("[%s]    %s", now, column) }
    }

    // example listing at the super column level
    val recentInsert = session.list(superColumn)
    log("[%s] listed recent inserts, size: %s", now, recentInsert.size)
    recentInsert.foreach { (column) => log("[%s]  %s", now, column) }

  }

  implicit def columnToString(col:Column[_]):String = "%s -> %s (time: %s)".format(string(col.name), string(col.value), col.time)
  implicit def intToString(i:Int):String = Integer.toString(i)
  implicit def longToString(l:Long):String = java.lang.Long.toString(l)
  implicit def byteValueToString(obj:ByteValue):String = string(obj.value)
  implicit def stringValueToString(obj:StringValue):String = obj.value

}