package ai.h2o.sparkling.backend

import ai.h2o.sparkling.H2OConf

/**
  * Shared configuration independent on used backend
  */
trait SharedBackendConfExtensions {
  self: H2OConf =>

  import SharedBackendConf._

  private[backend] def getFileProperties: Seq[(String, _, _, _)] =
    Seq(PROP_JKS, PROP_LOGIN_CONF, PROP_SSL_CONF)

  protected def setBackendClusterMode(backendClusterMode: String) = {
    set(PROP_BACKEND_CLUSTER_MODE._1, backendClusterMode)
  }

  private[sparkling] def getClientLanguage: String = sparkConf.get(PROP_CLIENT_LANGUAGE._1, PROP_CLIENT_LANGUAGE._2)

}
