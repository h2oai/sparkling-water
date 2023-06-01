package ai.h2o.sparkling.backend.exceptions

import scala.util.control.NoStackTrace

class WrongSparkVersion(msg: String) extends Exception(msg) with NoStackTrace
