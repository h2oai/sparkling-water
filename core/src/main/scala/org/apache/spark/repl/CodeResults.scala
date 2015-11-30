package org.apache.spark.repl

/**
  * Enum representing possible results of code interpreted in scala interpreter
  */
object CodeResults extends Enumeration {
val Success, Error, Incomplete, Exception = Value
}
