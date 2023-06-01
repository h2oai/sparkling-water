package ai.h2o.sparkling.utils

import scala.util.control.NonFatal

object ScalaUtils {
  def withResource[T <: AutoCloseable, R](resource: T)(body: T => R): R = {
    val evaluatedResource: T = resource
    require(resource != null, "The auto-closable resource can't be null!")
    var exception: Throwable = null
    try {
      body(evaluatedResource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      closeResource(evaluatedResource, exception)
    }
  }

  private def closeResource(resource: AutoCloseable, e: Throwable): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) => e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }
}
