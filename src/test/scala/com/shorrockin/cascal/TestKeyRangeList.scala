package com.shorrockin.cascal

import model.{ColumnFamily, Key, StandardKey}
import org.junit.{Assert, Test}

class TestKeyRangeList extends EmbeddedCassandra {
  import Assert._
  import Conversions._

  @Test def testKeyRangeList = borrow { (s) =>
    val key1 = "Test" \ "Standard" \ "1"
    val key2 = "Test" \ "Standard" \ "2"
    val key3 = "Test" \ "Standard" \ "3"
    val key4 = "Test" \ "Standard" \ "4"

    def insert(key:StandardKey) = s.insert(key \ ("Hello", "World"))
    insert(key1)
    insert(key2)
    insert(key3)
    insert(key4)


    val results = s.list(key1.family, KeyRange("1", "3", 100), EmptyPredicate, Consistency.One)
    assertEquals(3, results.size)
  }
}