package ai.h2o.sparkling.backend.external

import java.io.{ByteArrayInputStream, InputStream}
import java.util.concurrent.TimeUnit

import ai.h2o.sparkling.H2OConf
import io.fabric8.kubernetes.client.KubernetesClient

trait K8sHeadlessService extends K8sUtils {

  protected def getH2OHeadlessServiceURL(conf: H2OConf): String = {
    s"${conf.externalK8sH2OServiceName}.${conf.externalK8sNamespace}.svc.${conf.externalK8sDomain}"
  }

  protected def installH2OHeadlessService(client: KubernetesClient, conf: H2OConf): Unit = {
    val service = client.services().load(spec(conf)).get
    client.services().inNamespace(conf.externalK8sNamespace).createOrReplace(service)
  }

  protected def deleteH2OHeadlessService(client: KubernetesClient, conf: H2OConf): Unit = {
    client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .withName(conf.externalK8sH2OServiceName)
      .delete()
  }

  private def spec(conf: H2OConf): InputStream = {
    val spec = s"""
      |apiVersion: v1
      |kind: Service
      |metadata:
      |  name: ${conf.externalK8sH2OServiceName}
      |  namespace: ${conf.externalK8sNamespace}
      |spec:
      |  type: ClusterIP
      |  clusterIP: None
      |  selector:
      |    ${convertLabel(conf.externalK8sH2OLabel)}
      |  ports:
      |  - protocol: TCP
      |    port: 54321""".stripMargin
    new ByteArrayInputStream(spec.getBytes)
  }
}
