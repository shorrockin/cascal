package com.shorrockin.cascal.model

/**
 * a super standard key the key who's parent is a super key. It acts in much
 * the same fashion as a standard key except for the parent structure is a SuperKey,
 * and not a StandardColumnFamily
 */
case class SuperColumn(val value:Array[Byte], val key:SuperKey) extends Gettable[Seq[Column[SuperColumn]]]()
                                                                   with StandardColumnContainer[Column[SuperColumn], Seq[Column[SuperColumn]]] {
  def \(name:Array[Byte]) = new Column(name, this)
  def \(name:Array[Byte], value:Array[Byte]) = new Column(name, value, this)
  def \(name:Array[Byte], value:Array[Byte], time:Long) = new Column(name, value, time, this)

  val family = key.family
  val keyspace = family.keyspace
}