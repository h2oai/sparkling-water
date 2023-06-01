package ai.h2o.sparkling.backend

object H2OJobStatus extends Enumeration {
  val DONE, CANCELLED, FAILED, RUNNING = Value

  def fromString(status: String): Value = {
    values.find(_.toString == status).getOrElse(throw new RuntimeException(s"Unknown H2O's Job status $status"))
  }
}
