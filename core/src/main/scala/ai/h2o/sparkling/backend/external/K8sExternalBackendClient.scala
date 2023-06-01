package ai.h2o.sparkling.backend.external

import ai.h2o.sparkling.H2OConf
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import org.apache.spark.expose.Logging

object K8sExternalBackendClient extends K8sHeadlessService with K8sH2OStatefulSet with Logging {

  def stopExternalH2OOnKubernetes(conf: H2OConf): Unit = {
    val client = new DefaultKubernetesClient
    deleteH2OHeadlessService(client, conf)
    deleteH2OStatefulSet(client, conf)
  }

  def startExternalH2OOnKubernetes(conf: H2OConf): Unit = {
    val client = new DefaultKubernetesClient
    stopExternalH2OOnKubernetes(conf)
    installH2OHeadlessService(client, conf)
    installH2OStatefulSet(client, conf, getH2OHeadlessServiceURL(conf))
    conf.setH2OCluster(s"${getH2OHeadlessServiceURL(conf)}:54321")
  }
}
