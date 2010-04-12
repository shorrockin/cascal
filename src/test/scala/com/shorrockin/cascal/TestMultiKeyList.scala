package com.shorrockin.cascal

import model._
import testing._
import org.junit.{Assert, Test}
import session.{Session, ColumnPredicate, Order, RangePredicate}
import utils.{UUID, Conversions}

/**
 * tests our ability to list multiple keys, translates to a cassandra
 * multiget_slice
 */
class TestMultiKeyList extends CassandraTestPool {
  import Assert._
  import Conversions._

  def locate(results:Seq[(ColumnContainer[_, _], Seq[Column[_]])], key:ColumnContainer[_, _]):Seq[Column[_]] = {
    results.find { _._1 == key }.get._2
  }

  @Test def testStandardMultiKeyRetrieval = borrow { (session) =>
    val key1 = populate("Test" \ "Standard" \ UUID(), session, 3)
    val key2 = populate("Test" \ "Standard" \ UUID(), session, 1)
    val key3 = populate("Test" \ "Standard" \ UUID(), session, 2)

    val results = session.list(key1 :: key3)
    assertEquals(2, results.size) // does the map have 2 keys?
    assertEquals(3, locate(results, key1).size) // does key1 have 3 columns?
    assertEquals(2, locate(results, key3).size) // does key3 have 2 columns?

    val key1Values = locate(results, key1).map { (col) => string(col.value)}
    
    log.debug("values of columns in Key-1: " + key1Values.mkString("", ",", ""))
    assertEquals(3, key1Values.size)
    assertTrue(key1Values.contains("val-0"))
    assertTrue(key1Values.contains("val-1"))
    assertTrue(key1Values.contains("val-2"))

    val key2Values = locate(results, key3).map { (col) => string(col.value)}
    log.debug("values of columns in Key-3: " + key2Values.mkString("", ",", ""))
    assertEquals(2, key2Values.size)
    assertTrue(key2Values.contains("val-0"))
    assertTrue(key2Values.contains("val-1"))
  }

  @Test def testStandardMultiKeyPredicateRetrieval = borrow { (session) =>
    val key1 = populate("Test" \ "Standard" \ UUID(), session, 3)
    val key2 = populate("Test" \ "Standard" \ UUID(), session, 1)
    val key3 = populate("Test" \ "Standard" \ UUID(), session, 2)

    // predicate is applied to the columns
    var results = session.list(key1 :: key2 :: key3, RangePredicate(Order.Descending, 2))
    assertEquals(3, results.size)
    assertEquals(2, locate(results, key1).size)
    assertEquals(1, locate(results, key2).size)
    assertEquals(2, locate(results, key3).size)

    // try out a column predicate
    val columns = List(bytes("col-1"), bytes("col-2"))
    results = session.list(key1 :: key2 :: key3, ColumnPredicate(columns))
    assertEquals(3, results.size)
    assertEquals(2, locate(results, key1).size)
    assertEquals(0, locate(results, key2).size)
    assertEquals(1, locate(results, key3).size)
  }


  @Test def testSuperMultiKeyPredicateRetrieval = borrow { (session) =>
    val key1 = populate("Test" \\ "SuperBytes" \ UUID(), session, 2, 3)
    val key2 = populate("Test" \\ "SuperBytes" \ UUID(), session, 3, 2)
    val key3 = populate("Test" \\ "SuperBytes" \ UUID(), session, 1, 1)
    assertTrue(true)
  }


  @Test def testSuperColumnMultiKeyRetrieval = borrow { session =>
    val key1 = populate("Test" \\ "SuperBytes" \ UUID(), session, 2, 3)
    val key2 = populate("Test" \\ "SuperBytes" \ UUID(), session, 3, 2)
    val key3 = populate("Test" \\ "SuperBytes" \ UUID(), session, 1, 1)

    // create 3 super columns under different keys
    val sc1  = key1 \ "sci-0"
    val sc2  = key2 \ "sci-0"
    val sc3  = key3 \ "sci-0"

    // should return a seq of tuples of (super column -> seq[column])
    val results = session.list(sc1 :: sc2 :: sc3)
    assertEquals(3, results.size)
  }

  def populate(key:SuperKey, session:Session, superAmount:Int, colAmount:Int):SuperKey = {
    (0 until superAmount).foreach { (sci) =>
      val superColumn = key \ ("sc-" + sci)

      (0 until colAmount).foreach { (i) =>
        val columnName = "col-" + i
        val columnVal  = "val-" + i
        session.insert(superColumn \ (columnName, columnVal))
      }
    }

    key
  }

  def populate(key:StandardKey, session:Session, amount:Int):StandardKey = {
    (0 until amount).foreach { (i) =>
      val columnName = "col-" + i
      val columnVal  = "val-" + i
      session.insert(key \ (columnName, columnVal))
    }

    key
  }

}
