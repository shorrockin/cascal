package com.shorrockin.cascal

import testing._
import model.Column
import org.junit.{Assert, Test}
import java.util.Date
import session.{ColumnPredicate, RangePredicate}
import utils.{Conversions, Logging}

class TestStandardInesrtAndList extends CassandraTestPool with Logging {
  import Assert._
  import Conversions._

  @Test def testStandardFunctions = borrow {
    (session) =>
      import session._

      log.debug("Cluster Name: " + clusterName)
      log.debug("Version: " + version)
      log.debug("Keyspaces: " + keyspaces.mkString("", ",", ""))

      val family = "Test" \ "Standard"
      val key = family \ "testStandardFunctions"

      insert(key \ "Column-1" \ "Value-1")
      insert(key \ "Column-2" \ "Value-2")
      insert(key \ "Column-3" \ "Value-3")
      insert(key \ "Column-4" \ "Value-4")

      val column = get(key \ "Column-1")
      assertNotNull(column)
      assertEquals("Value-1", string(column.get.value))
      assertEquals("Column-1", string(column.get.name))
      log.debug("get 'Column-1' returned: " + string(column.get))

      val columnCount = count(key)
      assertEquals(4, columnCount)

      var columns = list(key)
      assertEquals(4, columns.size)
      columns.foreach {(c) => log.debug("list(key) returned column: " + string(c))}

      columns = list(key, RangePredicate("Column-1", "Column-3"))
      assertEquals(3, columns.size)
      columns.foreach {(c) => log.debug("list(key, range) returned column: " + string(c))}

      columns = list(key, ColumnPredicate(List("Column-1", "Column-3")))
      assertEquals(2, columns.size)
      columns.foreach {(c) => log.debug("list(key, columns) returned column: " + string(c))}

      var failFamily = "Test" \ "Unstandard"
      var failFamily2 = "Test" \\ "Standard"
      var failFamily3 = "Tesst" \ "Standard"
      var fail1 = failFamily \ "fail1" \ "col1" \ "val1"
      var fail2 = failFamily2 \ "fail1" \ "super1" \ "col1" \ "val1"
      var fail3 = failFamily3 \ "fail1" \ "col1" \ "val1"
      try {
        insert(fail1)
        fail("Inserted into a non-existent columnfamily")
      } catch {
        case e: IllegalArgumentException => {}
      }
      try {
        insert(fail2)
        fail("Inserted a supercolumn in a standard family")
      } catch {
        case e: IllegalArgumentException => {}
      }
      try {
        insert(fail3)
        fail("Inserted into a non existent keyspace")
      } catch {
        case e: IllegalArgumentException => {}
      }


  }

}
