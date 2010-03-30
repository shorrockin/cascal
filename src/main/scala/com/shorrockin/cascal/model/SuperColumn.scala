package com.shorrockin.cascal.model

/**
 * a super standard key the key who's parent is a super key. It acts in much
 * the same fashion as a standard key except for the parent structure is a SuperKey,
 * and not a StandardColumnFamily
 */
case class SuperColumn(val value:Array[Byte], val key:SuperKey) extends ColumnName[Seq[ColumnValue[SuperColumn]]]()
                                                                   with ColumnContainer[StandardColumn[SuperColumn], Seq[ColumnValue[SuperColumn]]] {
  def \(value:Array[Byte]) = new StandardColumn[SuperColumn](value, this)
  val family = key.family
  val keyspace = family.keyspace
}