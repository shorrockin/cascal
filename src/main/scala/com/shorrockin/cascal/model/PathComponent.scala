package com.shorrockin.cascal.model

trait PathComponent[ValueType] { val value:ValueType }
trait ByteValue extends PathComponent[Array[Byte]]
trait StringValue extends PathComponent[String]