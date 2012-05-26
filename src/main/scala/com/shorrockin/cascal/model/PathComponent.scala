package com.shorrockin.cascal.model
import java.nio.ByteBuffer

/**
 * categorization of a cassandra path component.
 *
 * @author Chris Shorrock
 * @param ValueType all path components contain a value, this
 * defines the type of value.
 */
trait PathComponent[ValueType] { val value:ValueType }

/**
 * categorization of a path component who's value is a byte
 * @author Chris Shorrock
 */
trait ByteValue extends PathComponent[ByteBuffer]

/**
 * categorization of path component who's value is a string
 * @author Chris Shorrock
 */
trait StringValue extends PathComponent[String]
