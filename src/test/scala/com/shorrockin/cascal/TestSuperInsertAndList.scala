package com.shorrockin.cascal

import testing._
import model.{Column, SuperColumn}
import org.junit.{Test, Assert}
import utils.{UUID, Conversions, Logging}

class TestSuperInsertAndList extends CassandraTestPool with Logging {
  import Assert._
  import Conversions._

  @Test def testSuperFamily = borrow { (session) =>
    import session._

    val key          = "Test" \\ "Super" \ "1"
    val superColumn1 = key \ UUID()
    val column1      = superColumn1 \ "Column-1" \ "Value-1"
    val column2      = superColumn1 \ "Column-2" \ "Value-2"
    val column3      = superColumn1 \ "Column-3" \ "Value-3"
    val superColumn2 = key \ UUID()
    val column4      = superColumn2 \ "Column-4" \ "Value-4"
    val column5      = superColumn2 \ "Column-5" \ "Value-5"

    insert(column1)
    insert(column2)
    insert(column3)
    insert(column4)
    insert(column5)

    var column = get(column1)
    assertNotNull(column)
    assertEquals("Value-1", string(column.get.value))
    assertEquals("Column-1", string(column.get.name))
    log.debug("get 'Column-1' returned: " + string(column.get))

    var columns = get(superColumn1).get
    var values = columns.map { (c) => string(c.value) }
    assertEquals(3, columns.size)
    assertTrue(values.contains("Value-1"))
    assertTrue(values.contains("Value-3"))
    assertFalse(values.contains("Value-4"))
    log.debug("get(superColumn1) returned: " + values.mkString("", ",", ""))

    var cnt = count(key)
    assertEquals(2, cnt)
    log.debug("get(key) on super family returned 2 value")

    cnt = count(superColumn1)
    assertEquals(3, cnt)
    log.debug("get(super column) on super family returned 3 values")

    columns = list(superColumn2)
    values = columns.map { (c) => string(c.value) }
    log.debug("get(superColumn2) returned: " + values.mkString("", ",", ""))
    assertEquals(2, columns.size)
    assertFalse(values.contains("Value-1"))
    assertTrue(values.contains("Value-4"))

    val superKeyMap = list(key)
    log.debug("listing all results for list(key)")
    superKeyMap.foreach { (sc) =>
      log.debug("list(key) super column: " + UUID(sc._1.value))
      superKeyMap(sc._1).foreach { (column) => log.debug("  " + string(column)) }
    }
    assertEquals(2, superKeyMap.size)
    assertEquals(3, locate(superKeyMap, superColumn1.value).size)
    assertEquals(2, locate(superKeyMap, superColumn2.value).size)
  }


  def locate(map:Map[SuperColumn, Seq[Column[_]]], value:Array[Byte]):Seq[Column[_]] = {
    map.foreach { (tuple) =>
      if (java.util.Arrays.equals(tuple._1.value, value)) {
        return tuple._2
      }
    }
    throw new NoSuchElementException("could not find value in map")
  }
}
