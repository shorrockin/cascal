package com.shorrockin.cascal

import org.junit.Test
import jmx.CascalStatistics

class TestCascalStatistics {

  @Test def testMultipleMBeanRegistration() {
    // if recalling this doesn't throw an exception - we treat that as success
    CascalStatistics.reinstallMBean
    CascalStatistics.reinstallMBean
  }

}