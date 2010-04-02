package com.shorrockin.cascal

import model.Column
import org.junit.{Assert, Test}

class TestBatchOperations extends EmbeddedCassandra {
  import Conversions._
  import Assert._

  @Test def testStandardBatchInsertion = withSession { (s) =>
    val key  = "Test" \ "Standard" \ UUID()
    val col1 = key \ ("Column-1", "Value-1")
    val col2 = key \ ("Column-2", "Value-2")
    val col3 = key \ ("Column-3", "Value-3")

    s.batch(Insert(col1) :: Insert(col2) :: Insert(col3))

    assertEquals(3, s.list(col1.owner).size)
  }

  @Test def testSuperBatchInsertion = withSession { (s) =>
    val key1 = "Test" \\ "Super" \ UUID()
    val key2 = "Test" \\ "Super" \ UUID()
    val sc1  = key1 \ UUID()
    val sc12 = key1 \ UUID()
    val sc2  = key2 \ UUID()
    val col1 = sc1  \ ("Col1", "Val1")
    val col2 = sc1  \ ("Col2", "Val2")
    val col3 = sc12 \ ("Col3", "Val3")
    val col4 = sc2  \ ("Col4", "Val4")

    s.batch(Insert(col1) :: Insert(col2) :: Insert(col3) :: Insert(col4))

    assertEquals(2, s.list(sc1).size)
    assertEquals(1, s.list(sc12).size)
    assertEquals(1, s.list(sc2).size)

    val superCols = s.list(key1)
    val combined = superCols.foldLeft(List[Column[_]]()) { (left, right) => right._2.toList ::: left}
    assertEquals(2, superCols.size)
    assertEquals(3, combined.toList.size)
  }

  @Test def testStandardBatchDelete = withSession { (s) =>
    val key  = "Test" \ "Standard" \ UUID()
    val col1 = key \ ("Column-1", "Value-1")
    val col2 = key \ ("Column-2", "Value-2")
    val col3 = key \ ("Column-3", "Value-3")
    val col4 = key \ ("Column-4", "Value-4")

    s.batch(Insert(col1) :: Insert(col2) :: Insert(col3))
    assertEquals(3, s.list(key).size)

    s.batch(Delete(key, ColumnPredicate(col2.name :: col3.name :: Nil)) :: Insert(col4))

    assertEquals(2, s.list(key).size)
    assertEquals(None, s.get(col2))
    assertEquals(None, s.get(col3))
    assertEquals("Value-1", string(s.get(col1).get.value))
    assertEquals("Value-4", string(s.get(col4).get.value))
  }

  @Test def testBatchSuperDelete = withSession { (s) =>
    val key1 = "Test" \\ "Super" \ UUID()
    val sc1  = key1 \ UUID()
    val sc2  = key1 \ UUID()
    val col1 = sc1  \ ("Col1", "Val1")
    val col2 = sc1  \ ("Col2", "Val2")
    val col3 = sc2  \ ("Col3", "Val3")

    s.batch(Insert(col1) :: Insert(col2) :: Insert(col3))

    assertEquals(2, s.list(key1).size)
    assertEquals(2, s.list(sc1).size)
    assertEquals(1, s.list(sc2).size)

    s.batch(Delete(sc2) :: Nil)
    assertEquals(0, s.list(sc2).size)

    /* TODO wierdness within
    s.batch(Delete(key1, ColumnPredicate(col1.name :: col2.name :: Nil)) :: Nil)

    assertEquals(0, s.list(sc1).size)
    assertEquals(0, s.list(key1).size)
    */
  }
}