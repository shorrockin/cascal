package com.shorrockin.cascal.model

/**
 * a super standard key the key who's parent is a super key. It acts in much
 * the same fashion as a standard key except for the parent structure is a SuperKey,
 * and not a StandardColumnFamily
 */
case class SuperColumn(val value:Array[Byte], val key:SuperKey) extends Column[Seq[StandardColumn[SuperColumn]]]()
                                                                   with StandardColumnContainer[StandardColumn[SuperColumn], Seq[StandardColumn[SuperColumn]]] {
  def \(name:Array[Byte]) = new StandardColumn(name, this)
  def \(name:Array[Byte], value:Array[Byte]) = new StandardColumn(name, value, this)
  def \(name:Array[Byte], value:Array[Byte], time:Long) = new StandardColumn(name, value, time, this)

  val family = key.family
  val keyspace = family.keyspace
}