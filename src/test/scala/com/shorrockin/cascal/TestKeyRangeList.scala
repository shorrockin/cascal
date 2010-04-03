package com.shorrockin.cascal

import model.{ColumnFamily, Key, StandardKey}
import org.junit.{Assert, Test}
import session.{Consistency, EmptyPredicate, KeyRange}
import utils.Conversions

class TestKeyRangeList extends CassandraTestPool {
  import Assert._
  import Conversions._

  @Test def testKeyRangeList = borrow { (s) =>
    val key1 = "Test" \ "Standard" \ "testKeyRangeList-1"
    val key2 = "Test" \ "Standard" \ "testKeyRangeList-2"
    val key3 = "Test" \ "Standard" \ "testKeyRangeList-3"
    val key4 = "Test" \ "Standard" \ "testKeyRangeList-4"

    def insert(key:StandardKey) = s.insert(key \ ("Hello", "World"))
    insert(key1)
    insert(key2)
    insert(key3)
    insert(key4)

    val results = s.list(key1.family, KeyRange("testKeyRangeList-1", "testKeyRangeList-3", 100), EmptyPredicate, Consistency.One)
    assertEquals(3, results.size)
  }
}