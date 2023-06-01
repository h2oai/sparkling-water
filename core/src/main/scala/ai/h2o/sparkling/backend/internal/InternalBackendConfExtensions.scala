package ai.h2o.sparkling.backend.internal

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.SharedBackendConf

/**
  * Internal backend configuration
  */
trait InternalBackendConfExtensions extends SharedBackendConf {
  self: H2OConf =>

  import InternalBackendConf._

  def internalConfString: String =
    s"""Sparkling Water configuration:
       |  backend cluster mode : $backendClusterMode
       |  workers              : $numH2OWorkers
       |  cloudName            : ${cloudName.getOrElse(
         "Not set yet, it will be set automatically before starting H2OContext.")}
       |  base port            : $basePort
       |  cloudTimeout         : $cloudTimeout
       |  log level            : $logLevel
       |  nthreads             : $nthreads
       |  drddMulFactor        : $drddMulFactor""".stripMargin

  private[backend] override def getFileProperties: Seq[(String, _, _, _)] = super.getFileProperties :+ PROP_HDFS_CONF
}
