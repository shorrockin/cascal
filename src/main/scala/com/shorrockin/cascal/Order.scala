package com.shorrockin.cascal

object Order {
  val Ascending = new Order { def reversed = false }
  val Descending = new Order { def reversed = true }
}

trait Order {
  def reversed:Boolean
}