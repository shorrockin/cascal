package com.shorrockin.cascal.model

/**
 * implementation of a standard key, which is an object which can be thought
 * of as a container for a list of columns. The parent of this will either
 * be a StandardColumnFamily or a SuperKey
 *
 * @author Chris Shorrock
 */
case class StandardKey(val value:String, val family:StandardColumnFamily) extends Key {
  def \(value:Array[Byte]) = new StandardColumn[StandardKey](value, this)
}