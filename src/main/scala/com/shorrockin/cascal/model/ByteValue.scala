package com.shorrockin.cascal.model

/**
 * defines a cassandra component type who's value is composed of
 * bytes
 */
trait ByteValue extends PathComponent[Array[Byte]]