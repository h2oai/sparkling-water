package ai.h2o.sparkling.backend.exceptions

abstract class RestApiException(msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
}
