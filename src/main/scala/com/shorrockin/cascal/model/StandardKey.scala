package com.shorrockin.cascal.model

/**
 * implementation of a standard key, which is an object which can be thought
 * of as a container for a list of columns. The parent of this will either
 * be a StandardColumnFamily or a SuperKey
 *
 * @author Chris Shorrock
 */
case class StandardKey(val value:String, val family:StandardColumnFamily) extends Key[StandardColumn[StandardKey], Seq[StandardColumn[StandardKey]]]
                                                                             with StandardColumnContainer[StandardColumn[StandardKey], Seq[StandardColumn[StandardKey]]] {
  def \(name:Array[Byte]) = new StandardColumn(name, this)
  def \(name:Array[Byte], value:Array[Byte]) = new StandardColumn(name, value, this)
  def \(name:Array[Byte], value:Array[Byte], time:Long) = new StandardColumn(name, value, time, this)
}