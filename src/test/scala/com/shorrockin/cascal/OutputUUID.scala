package com.shorrockin.cascal

object OutputUUID {
  def main(args:Array[String]) {
    val uuid = UUID.uuid
    System.out.println("Byte Length: " + UUID.toBytes(uuid).length)
    System.out.println("UUID: " + uuid)
  }
}