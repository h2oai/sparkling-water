package ai.h2o.sparkling.repl

/**
  * Enum representing possible results of code interpreted in scala interpreter
  */
object CodeResults extends Enumeration {
  val Success, Error, Incomplete, Exception = Value
}
