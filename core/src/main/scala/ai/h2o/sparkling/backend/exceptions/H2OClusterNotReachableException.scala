package ai.h2o.sparkling.backend.exceptions

class H2OClusterNotReachableException(msg: String, cause: Throwable) extends RestApiException(msg, cause) {
  def this(msg: String) = this(msg, null)
}
