package com.shorrockin.cascal.model

/**
 * a cassandra component type, holds true for all types
 */
trait PathComponent[ValueType] {
  val value:ValueType
}