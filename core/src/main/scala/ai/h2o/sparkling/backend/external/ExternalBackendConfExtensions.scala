package ai.h2o.sparkling.backend.external

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.backend.external.ExternalBackendConf.PROP_EXTERNAL_DISABLE_VERSION_CHECK
import org.apache.spark.expose.Logging

/**
  * External backend configuration
  */
trait ExternalBackendConfExtensions extends SharedBackendConf with Logging {
  self: H2OConf =>

  def externalConfString: String =
    s"""Sparkling Water configuration:
       |  backend cluster mode : $backendClusterMode
       |  cluster start mode   : $clusterStartMode
       |  cloudName            : ${cloudName.getOrElse("Not set yet")}
       |  cloud representative : ${h2oCluster.getOrElse("Not set, using cloud name only")}
       |  base port            : $basePort
       |  log level            : $logLevel
       |  nthreads             : $nthreads""".stripMargin

  private[backend] def isBackendVersionCheckDisabled =
    sparkConf.getBoolean(PROP_EXTERNAL_DISABLE_VERSION_CHECK._1, PROP_EXTERNAL_DISABLE_VERSION_CHECK._2)

}
