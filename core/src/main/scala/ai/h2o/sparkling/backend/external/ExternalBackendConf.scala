/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.h2o.sparkling.backend.external

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.H2OConf.{BooleanOption, IntOption, OptionOption, StringOption}
import ai.h2o.sparkling.backend.{BuildInfo, SharedBackendConf}
import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.utils.Compression
import org.apache.spark.expose.Logging

import scala.collection.JavaConverters._

/**
  * External backend configuration
  */
trait ExternalBackendConf extends SharedBackendConf with Logging with ExternalBackendConfExtensions {
  self: H2OConf =>

  import ExternalBackendConf._

  /** Getters */
  def h2oCluster: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)

  def h2oClusterHost: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1).map(_.split(":")(0))

  def h2oClusterPort: Option[Int] =
    sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1).map(_.split(":")(1).toInt)

  def clusterSize: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_SIZE._1)

  def clusterStartTimeout: Int =
    sparkConf.getInt(PROP_EXTERNAL_CLUSTER_START_TIMEOUT._1, PROP_EXTERNAL_CLUSTER_START_TIMEOUT._2)

  def clusterInfoFile: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_INFO_FILE._1)

  @DeprecatedMethod("externalMemory", "3.34")
  def mapperXmx: String = externalMemory

  def externalMemory: String = sparkConf.get(PROP_EXTERNAL_MEMORY._1, PROP_EXTERNAL_MEMORY._2)

  def HDFSOutputDir: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1)

  def isAutoClusterStartUsed: Boolean = clusterStartMode == EXTERNAL_BACKEND_AUTO_MODE

  def isManualClusterStartUsed: Boolean = clusterStartMode == EXTERNAL_BACKEND_MANUAL_MODE

  def clusterStartMode: String = sparkConf.get(PROP_EXTERNAL_CLUSTER_START_MODE._1, PROP_EXTERNAL_CLUSTER_START_MODE._2)

  def h2oDriverPath: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1)

  def YARNQueue: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1)

  def isKillOnUnhealthyClusterEnabled: Boolean =
    sparkConf.getBoolean(PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._1, PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._2)

  def kerberosPrincipal: Option[String] = sparkConf.getOption(PROP_EXTERNAL_KERBEROS_PRINCIPAL._1)

  def kerberosKeytab: Option[String] = sparkConf.getOption(PROP_EXTERNAL_KERBEROS_KEYTAB._1)

  def runAsUser: Option[String] = sparkConf.getOption(PROP_EXTERNAL_RUN_AS_USER._1)

  def externalH2ODriverIf: Option[String] = sparkConf.getOption(PROP_EXTERNAL_DRIVER_IF._1)

  def externalH2ODriverPort: Option[String] = sparkConf.getOption(PROP_EXTERNAL_DRIVER_PORT._1)

  def externalH2ODriverPortRange: Option[String] = sparkConf.getOption(PROP_EXTERNAL_DRIVER_PORT_RANGE._1)

  def externalExtraMemoryPercent: Int =
    sparkConf.getInt(PROP_EXTERNAL_EXTRA_MEMORY_PERCENT._1, PROP_EXTERNAL_EXTRA_MEMORY_PERCENT._2)

  def externalBackendStopTimeout: Int =
    sparkConf.getInt(PROP_EXTERNAL_BACKEND_STOP_TIMEOUT._1, PROP_EXTERNAL_BACKEND_STOP_TIMEOUT._2)

  def externalHadoopExecutable: String =
    sparkConf.get(PROP_EXTERNAL_HADOOP_EXECUTABLE._1, PROP_EXTERNAL_HADOOP_EXECUTABLE._2)

  def externalExtraJars: Option[String] = sparkConf.getOption(PROP_EXTERNAL_EXTRA_JARS._1)

  def externalCommunicationCompression: String =
    sparkConf.get(PROP_EXTERNAL_COMMUNICATION_COMPRESSION._1, PROP_EXTERNAL_COMMUNICATION_COMPRESSION._2)

  def externalAutoStartBackend: String =
    sparkConf.get(PROP_EXTERNAL_AUTO_START_BACKEND._1, PROP_EXTERNAL_AUTO_START_BACKEND._2)

  def externalK8sH2OServiceName: String =
    sparkConf.get(PROP_EXTERNAL_K8S_H2O_SERVICE_NAME._1, PROP_EXTERNAL_K8S_H2O_SERVICE_NAME._2)

  def externalK8sH2OStatefulsetName: String =
    sparkConf.get(PROP_EXTERNAL_K8S_H2O_STATEFULSET_NAME._1, PROP_EXTERNAL_K8S_H2O_STATEFULSET_NAME._2)

  def externalK8sH2OLabel: String = sparkConf.get(PROP_EXTERNAL_K8S_H2O_LABEL._1, PROP_EXTERNAL_K8S_H2O_LABEL._2)

  def externalK8sH2OApiPort: Int =
    sparkConf.getInt(PROP_EXTERNAL_K8S_H2O_API_PORT._1, PROP_EXTERNAL_K8S_H2O_API_PORT._2)

  def externalK8sNamespace: String = sparkConf.get(PROP_EXTERNAL_K8S_NAMESPACE._1, PROP_EXTERNAL_K8S_NAMESPACE._2)

  def externalK8sDockerImage: String =
    sparkConf.get(PROP_EXTERNAL_K8S_DOCKER_IMAGE._1, PROP_EXTERNAL_K8S_DOCKER_IMAGE._2)

  def externalK8sDomain: String = sparkConf.get(PROP_EXTERNAL_K8S_DOMAIN._1, PROP_EXTERNAL_K8S_DOMAIN._2)

  def externalK8sServiceTimeout: Int =
    sparkConf.getInt(PROP_EXTERNAL_K8S_SERVICE_TIMEOUT._1, PROP_EXTERNAL_K8S_SERVICE_TIMEOUT._2)

  /** Setters */
  /**
    * Sets node and port representing H2O Cluster to which should H2O connect when started in external mode.
    * This method automatically sets external cluster mode
    *
    * @param host host representing the cluster
    * @param port port representing the cluster
    * @return H2O Configuration
    */
  def setH2OCluster(host: String, port: Int): H2OConf = {
    set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, host + ":" + port)
  }

  def setH2OCluster(hostPort: String): H2OConf = {
    set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, hostPort)
  }

  def setClusterSize(clusterSize: Int): H2OConf = set(PROP_EXTERNAL_CLUSTER_SIZE._1, clusterSize.toString)

  def setClusterStartTimeout(clusterStartTimeout: Int): H2OConf =
    set(PROP_EXTERNAL_CLUSTER_START_TIMEOUT._1, clusterStartTimeout.toString)

  def setClusterInfoFile(path: String): H2OConf = set(PROP_EXTERNAL_CLUSTER_INFO_FILE._1, path)

  @DeprecatedMethod("setExternalMemory", "3.34")
  def setMapperXmx(mem: String): H2OConf = setExternalMemory(mem)

  def setExternalMemory(memory: String): H2OConf = set(PROP_EXTERNAL_MEMORY._1, memory)

  def setHDFSOutputDir(dir: String): H2OConf = set(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1, dir)

  def useAutoClusterStart(): H2OConf = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_START_MODE._1, "auto")
  }

  def useManualClusterStart(): H2OConf = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_START_MODE._1, "manual")
  }

  def setH2ODriverPath(path: String): H2OConf = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1, path)
  }

  def setYARNQueue(queueName: String): H2OConf = set(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1, queueName)

  def setKillOnUnhealthyClusterEnabled(): H2OConf = set(PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._1, value = true)

  def setKillOnUnhealthyClusterDisabled(): H2OConf = set(PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._1, value = false)

  def setKerberosPrincipal(principal: String): H2OConf = set(PROP_EXTERNAL_KERBEROS_PRINCIPAL._1, principal)

  def setKerberosKeytab(path: String): H2OConf = set(PROP_EXTERNAL_KERBEROS_KEYTAB._1, path)

  def setRunAsUser(user: String): H2OConf = set(PROP_EXTERNAL_RUN_AS_USER._1, user)

  def setExternalH2ODriverIf(iface: String): H2OConf = set(PROP_EXTERNAL_DRIVER_IF._1, iface)

  def setExternalH2ODriverPort(port: Int): H2OConf = set(PROP_EXTERNAL_DRIVER_PORT._1, port.toString)

  def setExternalH2ODriverPortRange(portRange: String): H2OConf = set(PROP_EXTERNAL_DRIVER_PORT_RANGE._1, portRange)

  def setExternalExtraMemoryPercent(memoryPercent: Int): H2OConf =
    set(PROP_EXTERNAL_EXTRA_MEMORY_PERCENT._1, memoryPercent.toString)

  def setExternalBackendStopTimeout(timeout: Int): H2OConf =
    set(PROP_EXTERNAL_BACKEND_STOP_TIMEOUT._1, timeout.toString)

  def setExternalHadoopExecutable(executable: String): H2OConf = set(PROP_EXTERNAL_HADOOP_EXECUTABLE._1, executable)

  def setExternalExtraJars(commaSeparatedPaths: String): H2OConf = set(PROP_EXTERNAL_EXTRA_JARS._1, commaSeparatedPaths)

  def setExternalExtraJars(paths: java.util.ArrayList[String]): H2OConf = setExternalExtraJars(paths.asScala)

  def setExternalExtraJars(paths: Seq[String]): H2OConf = setExternalExtraJars(paths.mkString(","))

  def setExternalCommunicationCompression(compressionType: String): H2OConf = {
    set(PROP_EXTERNAL_COMMUNICATION_COMPRESSION._1, compressionType)
  }

  def setExternalAutoStartBackend(backend: String): H2OConf = {
    if (!Array(YARN_BACKEND, KUBERNETES_BACKEND).contains(backend)) {
      throw new IllegalArgumentException(
        "Backend for auto start mode of external H2O backend can be either" +
          s" $YARN_BACKEND or $KUBERNETES_BACKEND")
    }
    set(PROP_EXTERNAL_AUTO_START_BACKEND._1, backend)
  }

  def setExternalK8sH2OServiceName(serviceName: String): H2OConf = {
    set(PROP_EXTERNAL_K8S_H2O_SERVICE_NAME._1, serviceName)
  }

  def setExternalK8sH2OStatefulsetName(statefulsetName: String): H2OConf = {
    set(PROP_EXTERNAL_K8S_H2O_STATEFULSET_NAME._1, statefulsetName)
  }

  def setExternalK8sH2OLabel(label: String): H2OConf = {
    set(PROP_EXTERNAL_K8S_H2O_LABEL._1, label)
  }

  def setExternalK8sH2OApiPort(port: Int): H2OConf = {
    set(PROP_EXTERNAL_K8S_H2O_API_PORT._1, port.toString)
  }

  def setExternalK8sNamespace(namespace: String): H2OConf = {
    set(PROP_EXTERNAL_K8S_NAMESPACE._1, namespace)
  }

  def setExternalK8sDockerImage(name: String): H2OConf = {
    set(PROP_EXTERNAL_K8S_DOCKER_IMAGE._1, name)
  }

  def setExternalK8sDomain(domain: String): H2OConf = {
    set(PROP_EXTERNAL_K8S_DOMAIN._1, domain)
  }

  def setExternalK8sServiceTimeout(timeout: Int): H2OConf = {
    set(PROP_EXTERNAL_K8S_SERVICE_TIMEOUT._1, timeout.toString)
  }
}

object ExternalBackendConf {

  val EXTERNAL_BACKEND_AUTO_MODE: String = "auto"
  val EXTERNAL_BACKEND_MANUAL_MODE: String = "manual"

  val YARN_BACKEND: String = "yarn"
  val KUBERNETES_BACKEND: String = "kubernetes"

  val PROP_EXTERNAL_DRIVER_IF: OptionOption = (
    "spark.ext.h2o.external.driver.if",
    None,
    "setExternalH2ODriverIf(String)",
    "Ip address or network of mapper->driver callback interface. Default value means automatic detection.")

  val PROP_EXTERNAL_DRIVER_PORT: OptionOption = (
    "spark.ext.h2o.external.driver.port",
    None,
    "setExternalH2ODriverPort(Integer)",
    "Port of mapper->driver callback interface. Default value means automatic detection.")

  val PROP_EXTERNAL_DRIVER_PORT_RANGE: OptionOption = (
    "spark.ext.h2o.external.driver.port.range",
    None,
    "setExternalH2ODriverPortRange(String)",
    "Range portX-portY of mapper->driver callback interface; eg: 50000-55000.")

  val PROP_EXTERNAL_EXTRA_MEMORY_PERCENT: IntOption = (
    "spark.ext.h2o.external.extra.memory.percent",
    10,
    "setExternalExtraMemoryPercent(Integer)",
    """This option is a percentage of external memory option and specifies memory
     |for internal JVM use outside of Java heap.""".stripMargin)

  val PROP_EXTERNAL_CLUSTER_REPRESENTATIVE: OptionOption = (
    "spark.ext.h2o.cloud.representative",
    None,
    "setH2OCluster(String)",
    "ip:port of a H2O cluster leader node to identify external H2O cluster.")

  val PROP_EXTERNAL_CLUSTER_SIZE: OptionOption = (
    "spark.ext.h2o.external.cluster.size",
    None,
    "setClusterSize(Integer)",
    "Number of H2O nodes to start when ``auto`` mode of the external backend is set.")

  val PROP_EXTERNAL_CLUSTER_START_TIMEOUT: IntOption = (
    "spark.ext.h2o.cluster.start.timeout",
    120,
    "setClusterStartTimeout(Integer)",
    "Timeout in seconds for starting H2O external cluster")

  val PROP_EXTERNAL_CLUSTER_INFO_FILE: OptionOption = (
    "spark.ext.h2o.cluster.info.name",
    None,
    "setClusterInfoFile(Integer)",
    "Full path to a file which is used as the notification file for the startup of external H2O cluster. ")

  val PROP_EXTERNAL_MEMORY: StringOption = (
    "spark.ext.h2o.external.memory",
    "6G",
    "setExternalMemory(String)",
    "Amount of memory assigned to each external H2O node")

  val PROP_EXTERNAL_CLUSTER_HDFS_DIR: OptionOption = (
    "spark.ext.h2o.external.hdfs.dir",
    None,
    "setHDFSOutputDir(String)",
    "Path to the directory on HDFS used for storing temporary files.")

  val PROP_EXTERNAL_CLUSTER_START_MODE: StringOption = (
    "spark.ext.h2o.external.start.mode",
    EXTERNAL_BACKEND_MANUAL_MODE,
    """useAutoClusterStart()
      |useManualClusterStart()""".stripMargin,
    """If this option is set to ``auto`` then H2O external cluster is automatically started using the
    |provided H2O driver JAR on YARN, otherwise it is expected that the cluster is started by the user
    |manually""".stripMargin)

  val PROP_EXTERNAL_CLUSTER_DRIVER_PATH: OptionOption = (
    "spark.ext.h2o.external.h2o.driver",
    None,
    "setH2ODriverPath(String)",
    " Path to H2O driver used during ``auto`` start mode.")

  val PROP_EXTERNAL_CLUSTER_YARN_QUEUE: OptionOption = (
    "spark.ext.h2o.external.yarn.queue",
    None,
    "setYARNQueue(String)",
    "Yarn queue on which external H2O cluster is started.")

  val PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY: BooleanOption = (
    "spark.ext.h2o.external.kill.on.unhealthy",
    true,
    """setKillOnUnhealthyClusterEnabled()
      |setKillOnUnhealthyClusterDisabled()""".stripMargin,
    """If true, the client will try to kill the cluster and then itself in
      |case some nodes in the cluster report unhealthy status.""".stripMargin)

  val PROP_EXTERNAL_KERBEROS_PRINCIPAL: OptionOption =
    ("spark.ext.h2o.external.kerberos.principal", None, "setKerberosPrincipal(String)", "Kerberos Principal")

  val PROP_EXTERNAL_KERBEROS_KEYTAB: OptionOption =
    ("spark.ext.h2o.external.kerberos.keytab", None, "setKerberosKeytab(String)", "Kerberos Keytab")

  val PROP_EXTERNAL_RUN_AS_USER: OptionOption =
    ("spark.ext.h2o.external.run.as.user", None, "setRunAsUser(String)", "Impersonated Hadoop user")

  val PROP_EXTERNAL_BACKEND_STOP_TIMEOUT: IntOption = (
    "spark.ext.h2o.external.backend.stop.timeout",
    10000,
    "setExternalBackendStopTimeout(Integer)",
    """Timeout for confirmation from worker nodes when stopping the  external backend. It is also
     |possible to pass ``-1`` to ensure the indefinite timeout. The unit is milliseconds.""".stripMargin)

  val PROP_EXTERNAL_HADOOP_EXECUTABLE: StringOption = (
    "spark.ext.h2o.external.hadoop.executable",
    "hadoop",
    "setExternalHadoopExecutable(String)",
    """Name or path to path to a hadoop  executable binary which is used
      |to start external H2O backend on YARN.""".stripMargin)

  val PROP_EXTERNAL_EXTRA_JARS: OptionOption = (
    "spark.ext.h2o.external.extra.jars",
    None,
    """setExternalExtraJars(String)
      |setExternalExtraJars(String[])""".stripMargin,
    "Comma-separated paths to jars that will be placed onto classpath of each H2O node.")

  val PROP_EXTERNAL_COMMUNICATION_COMPRESSION: StringOption = (
    "spark.ext.h2o.external.communication.compression",
    Compression.defaultCompression,
    "setExternalCommunicationCompression(String)",
    """The type of compression used for data transfer between Spark and H2O node.
    |Possible values are ``NONE``, ``DEFLATE``, ``GZIP``, ``SNAPPY``.""".stripMargin)

  val PROP_EXTERNAL_AUTO_START_BACKEND: StringOption = (
    "spark.ext.h2o.external.auto.start.backend",
    YARN_BACKEND,
    "setExternalAutoStartBackend(String)",
    """The backend on which the external H2O backend will be started in auto start mode.
      |Possible values are ``YARN`` and ``KUBERNETES``.""".stripMargin)

  val PROP_EXTERNAL_K8S_H2O_SERVICE_NAME: StringOption = (
    "spark.ext.h2o.external.k8s.h2o.service.name",
    "h2o-service",
    "setExternalK8sH2OServceName(String)",
    "Name of H2O service required to start H2O on K8s.")

  val PROP_EXTERNAL_K8S_H2O_STATEFULSET_NAME: StringOption = (
    "spark.ext.h2o.external.k8s.h2o.statefulset.name",
    "h2o-statefulset",
    "setExternalK8sH2OStatefulsetName(String)",
    "Name of H2O stateful set required to start H2O on K8s.")

  val PROP_EXTERNAL_K8S_H2O_LABEL: StringOption = (
    "spark.ext.h2o.external.k8s.h2o.label",
    "app=h2o",
    "setExternalK8sH2OLabel(String)",
    "Label used to select node for H2O cluster formation.")

  val PROP_EXTERNAL_K8S_H2O_API_PORT: IntOption =
    ("spark.ext.h2o.external.k8s.h2o.api.port", 8081, "setExternalK8sH2OApiPort(String)", "Kubernetes API port.")

  val PROP_EXTERNAL_K8S_NAMESPACE: StringOption = (
    "spark.ext.h2o.external.k8s.namespace",
    "default",
    "setExternalK8sNamespace(String)",
    "Kubernetes namespace where external H2O is started.")

  val PROP_EXTERNAL_K8S_DOCKER_IMAGE: StringOption = (
    "spark.ext.h2o.external.k8s.docker.image",
    s"""See doc""",
    "setExternalK8sDockerImage(String)",
    s"Docker image containing Sparkling Water external H2O backend. Default value is h2oai/sparkling-water-external-backend:${BuildInfo.SWVersion}")

  val PROP_EXTERNAL_K8S_DOMAIN: StringOption = (
    "spark.ext.h2o.external.k8s.domain",
    s"cluster.local",
    "setExternalK8sDomain(String)",
    "Domain of the Kubernetes cluster.")

  val PROP_EXTERNAL_K8S_SERVICE_TIMEOUT: IntOption = (
    "spark.ext.h2o.external.k8s.svc.timeout",
    60 * 5,
    "setExternalK8sServiceTimeout(Int)",
    "Timeout in seconds used as a limit for K8s service creation.")

  private[sparkling] val PROP_EXTERNAL_DISABLE_VERSION_CHECK: (String, Boolean) =
    ("spark.ext.h2o.external.disable.version.check", false)

  private[sparkling] val PROP_EXTERNAL_CLUSTER_YARN_APP_ID: (String, None.type) =
    ("spark.ext.h2o.external.yarn.app.id", None)
}
