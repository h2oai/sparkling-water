package ai.h2o.sparkling.extensions.rest.api.schema

import water.{Key, Lockable}

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
