package ai.h2o.sparkling.backend.api.scalainterpreter


import water.{Key, Lockable};

/**
 * This object is returned by jobs executing the Scala code
 */
class ScalaCodeResult(key: Key[ScalaCodeResult]) extends Lockable[ScalaCodeResult](key) {
  var code: String = _
  var scalaStatus: String = _
  var scalaResponse: String = _
  var scalaOutput: String = _

  def this() = this(null)

  override def makeSchema(): Class[ScalaCodeResultV3] = {
    classOf[ScalaCodeResultV3]
  }
}
