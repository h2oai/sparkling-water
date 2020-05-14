package ai.h2o.sparkling.extensions.rest.api.schema

import water.{Iced, Key}
import water.api.schemas3.KeyV3

class ScalaCodeResultV3(key: Key[ScalaCodeResult])
  extends KeyV3[Iced[ScalaCodeResult], ScalaCodeResultV3, ScalaCodeResult](key) {
  def this() = this(null)
}
