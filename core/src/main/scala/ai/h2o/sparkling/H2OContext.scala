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

package ai.h2o.sparkling

import java.util.concurrent.atomic.AtomicReference

import ai.h2o.sparkling.backend._
import ai.h2o.sparkling.backend.converters._
import ai.h2o.sparkling.backend.exceptions.{H2OClusterNotReachableException, RestApiException, WrongSparkVersion}
import ai.h2o.sparkling.backend.external._
import ai.h2o.sparkling.backend.utils._
import ai.h2o.sparkling.utils.SparkSessionUtils
import javax.net.ssl.{HostnameVerifier, HttpsURLConnection, SSLSession}
import org.apache.spark.SparkContext
import org.apache.spark.expose.{Logging, Utils}
import org.apache.spark.h2o.SparkSpecificUtils
import org.apache.spark.h2o.backends.internal.InternalH2OBackend
import org.apache.spark.h2o.ui._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import water._
import water.util.PrettyPrint

import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Main entry point for Sparkling Water functionality. H2O Context represents connection to H2O cluster and allows us
  * to operate with it.
  *
  * H2O Context provides conversion methods from RDD/DataFrame to H2OFrame and back and also provides implicits
  * conversions for desired transformations
  *
  * Sparkling Water can run in two modes. External cluster mode and internal cluster mode. When using external cluster
  * mode, it tries to connect to existing H2O cluster using the provided spark
  * configuration properties. In the case of internal cluster mode,it creates H2O cluster living in Spark - that means
  * that each Spark executor will have one h2o instance running in it.  This mode is not
  * recommended for big clusters and clusters where Spark executors are not stable.
  *
  * Cluster mode can be set using the spark configuration
  * property spark.ext.h2o.mode which can be set in script starting sparkling-water or
  * can be set in H2O configuration class H2OConf
  */
/**
  * Create new H2OContext based on provided H2O configuration
  *
  * @param conf H2O configuration
  */
class H2OContext private[sparkling] (private val conf: H2OConf) extends H2OContextExtensions {
  self =>
  val sparkContext: SparkContext = SparkSessionUtils.active.sparkContext
  private val backendHeartbeatThread = createHeartBeatEventThread()

  private var stopped = false

  /** Here the client means Python or R H2O client */
  private var clientConnected = false

  /** Used backend */
  protected val backend: SparklingBackend = if (conf.runsInExternalClusterMode) {
    new ExternalH2OBackend(this)
  } else {
    new InternalH2OBackend(this)
  }
  H2OContext.logStartingInfo(conf)
  H2OContext.verifySparkVersion()
  backend.startH2OCluster(conf)
  logInfo("Connecting to H2O cluster.")
  private val nodes = getAndVerifyWorkerNodes(conf)
  backendHeartbeatThread.start() // start backend heartbeats
  if (H2OClientUtils.isH2OClientBased(conf)) {
    H2OClientUtils.startH2OClient(this, conf, nodes)
  }
  verifyExtensionJarIsAvailable()
  RestApiUtils.setTimeZone(conf, "UTC")
  // The lowest priority used by Spark is 25 (removing temp dirs). We need to perform cleaning up of H2O
  // resources before Spark does as we run as embedded application inside the Spark
  private val shutdownHookRef = Utils.addShutdownHook(10) { () =>
    logWarning("Spark shutdown hook called, stopping H2OContext!")
    stop(stopSparkContext = false, stopJvm = false, inShutdownHook = true)
  }
  if (conf.getBoolean("spark.ui.enabled", defaultValue = true)) {
    SparkSpecificUtils.addSparklingWaterTab(sparkContext)
  }

  private[sparkling] val (flowIp, flowPort) = {
    val uri = ProxyStarter.startFlowProxy(this, conf)
    // SparklyR implementation of Kubernetes connection works in a way that it does port-forwarding
    // from the driver node to the node where interactive session is running. We also need to make
    // sure that we provide valid ip. If this wouldn't be done, we would return dns record internal to
    // kubernetes
    if (conf.getClientLanguage == "r" && sparkContext.master.startsWith("k8s")) {
      ("127.0.0.1", uri.getPort)
    } else {
      (uri.getHost, uri.getPort)
    }
  }
  updateUIAfterStart() // updates the spark UI
  logInfo(s"Sparkling Water ${BuildInfo.SWVersion} started, status of context: $this ")

  def getH2ONodes(): Array[NodeDesc] = nodes

  private[this] def updateUIAfterStart(): Unit = {
    val cloudV3 = RestApiUtils.getClusterInfo(conf)
    val h2oBuildInfo = H2OBuildInfo(
      cloudV3.version,
      cloudV3.branch_name,
      cloudV3.last_commit_hash,
      cloudV3.describe,
      cloudV3.compiled_by,
      cloudV3.compiled_on)
    val h2oClusterInfo = H2OClusterInfo(
      s"$flowIp:$flowPort",
      cloudV3.cloud_healthy,
      cloudV3.internal_security_enabled,
      nodes.map(_.ipPort()),
      backend.backendUIInfo,
      cloudV3.cloud_uptime_millis)
    val propertiesDoc = collectPropertiesDoc()
    val swPropertiesInfo =
      conf.getAll.filter(_._1.startsWith("spark.ext.h2o")).map(p => (p._1, p._2, propertiesDoc.getOrElse(p._1, "")))

    val swHeartBeatEvent = getSparklingWaterHeartbeatEvent()
    Utils.postToListenerBus(swHeartBeatEvent)
    Utils.postToListenerBus(H2OContextStartedEvent(h2oClusterInfo, h2oBuildInfo, swPropertiesInfo))
  }

  /**
    * Return a copy of this H2OContext's configuration. The configuration ''cannot'' be changed at runtime.
    */
  def getConf: H2OConf = conf.clone()

  /** Transforms RDD[Supported type] to H2OFrame */
  def asH2OFrame(rdd: SupportedRDD): H2OFrame = asH2OFrame(rdd, None)

  def asH2OFrame(rdd: SupportedRDD, frameName: String): H2OFrame = asH2OFrame(rdd, Option(frameName))

  def asH2OFrame(rdd: SupportedRDD, frameName: Option[String]): H2OFrame = {
    withConversionDebugPrints(sparkContext, "SupportedRDD", {
      SupportedRDDConverter.toH2OFrame(this, rdd, frameName)
    })
  }

  /** Transform DataFrame to H2OFrame */
  def asH2OFrame(df: DataFrame): H2OFrame = asH2OFrame(df, None)

  def asH2OFrame(df: DataFrame, frameName: String): H2OFrame = asH2OFrame(df, Option(frameName))

  def asH2OFrame(df: DataFrame, frameName: Option[String]): H2OFrame = {
    withConversionDebugPrints(sparkContext, "Dataframe", SparkDataFrameConverter.toH2OFrame(this, df, frameName))
  }

  /** Transforms Dataset[Supported type] to H2OFrame */
  def asH2OFrame(ds: SupportedDataset): H2OFrame = asH2OFrame(ds, None)

  def asH2OFrame(ds: SupportedDataset, frameName: String): H2OFrame = asH2OFrame(ds, Option(frameName))

  def asH2OFrame(ds: SupportedDataset, frameName: Option[String]): H2OFrame = {
    withConversionDebugPrints(sparkContext, "SupportedDataset", {
      SupportedDatasetConverter.toH2OFrame(this, ds, frameName)
    })
  }

  /** Create a new H2OFrame based on existing Frame referenced by its id. */
  def asH2OFrame(s: String): H2OFrame = H2OFrame(s)

  /** Convert given H2O frame into a Product RDD type
    *
    * Consider using asH2OFrame since asRDD has several limitations such as that asRDD can't be used in Spark REPL
    * in case we are RDD[T] where T is class defined in REPL. This is because class T is created as inner class
    * and we are not able to create instance of class T without outer scope - which is impossible to get.
    * */
  def asRDD[A <: Product: TypeTag: ClassTag](fr: H2OFrame): RDD[A] = SupportedRDDConverter.toRDD[A](this, fr)

  /** A generic convert of Frame into Product RDD type
    *
    * Consider using asH2OFrame since asRDD has several limitations such as that asRDD can't be used in Spark REPL
    * in case we are RDD[T] where T is class defined in REPL. This is because class T is created as inner class
    * and we are not able to create instance of class T without outer scope - which is impossible to get.
    *
    * This code: hc.asRDD[PUBDEV458Type](rdd) will need to be call as hc.asRDD[PUBDEV458Type].apply(rdd)
    */
  def asRDD[A <: Product: TypeTag: ClassTag] = new {
    def apply(fr: H2OFrame): RDD[A] = SupportedRDDConverter.toRDD[A](H2OContext.this, fr)
  }

  def asSparkFrame(fr: H2OFrame, copyMetadata: Boolean = true): DataFrame = {
    SparkDataFrameConverter.toDataFrame(this, fr, copyMetadata)
  }

  def asSparkFrame(s: String, copyMetadata: Boolean): DataFrame = {
    SparkDataFrameConverter.toDataFrame(this, H2OFrame(s), copyMetadata)
  }

  def asSparkFrame(s: String): DataFrame = asSparkFrame(s, copyMetadata = true)

  /** Returns location of REST API of H2O client */
  def h2oLocalClient: String = flowIp + ":" + flowPort + conf.contextPath.getOrElse("")

  /** Returns IP of H2O client */
  def h2oLocalClientIp: String = flowIp

  /** Returns port where H2O REST API is exposed */
  def h2oLocalClientPort: Int = flowPort

  def setH2OLogLevel(level: String): Unit = {
    if (H2OClientUtils.isH2OClientBased(conf)) {
      water.util.Log.setLogLevel(level)
    }
    RestApiUtils.setLogLevel(conf, level)
  }

  def getH2OLogLevel(): String = RestApiUtils.getLogLevel(conf)

  private def stop(stopSparkContext: Boolean, stopJvm: Boolean, inShutdownHook: Boolean): Unit = synchronized {
    if (!inShutdownHook) {
      Utils.removeShutdownHook(shutdownHookRef)
    }
    if (!stopped) {
      backendHeartbeatThread.interrupt()
      if (stopSparkContext) {
        sparkContext.stop()
      }
      // Run orderly shutdown only in case of automatic mode of external backend, because:
      // In internal backend, Spark takes care of stopping executors automatically
      // In manual mode of external backend, the H2O cluster is managed by the user
      if (conf.runsInExternalClusterMode && conf.isAutoClusterStartUsed) {
        if (H2OClientUtils.isH2OClientBased(conf)) {
          H2O.orderlyShutdown(conf.externalBackendStopTimeout)
        } else {
          RestApiUtils.shutdownCluster(conf)
          if (conf.externalAutoStartBackend == ExternalBackendConf.KUBERNETES_BACKEND) {
            stopExternalH2OOnKubernetes(conf)
          }
        }
      }
      ProxyStarter.stopFlowProxy()
      H2OContext.instantiatedContext.set(null)
      stopped = true
      if (stopJvm && H2OClientUtils.isH2OClientBased(conf)) {
        H2O.exit(0)
      }
    } else {
      logWarning("H2OContext is already stopped!")
    }
  }

  /** Stops H2O context.
    *
    * @param stopSparkContext stop also spark context
    */
  def stop(stopSparkContext: Boolean = false): Unit = {
    stop(stopSparkContext, stopJvm = true, inShutdownHook = false)
  }

  def flowURL(): String = {
    if (AzureDatabricksUtils.isRunningOnAzureDatabricks(conf)) {
      AzureDatabricksUtils.flowURL(conf)
    } else if (conf.clientFlowBaseurlOverride.isDefined) {
      conf.clientFlowBaseurlOverride.get + conf.contextPath.getOrElse("")
    } else {
      "%s://%s:%d%s".format(conf.getScheme(), flowIp, flowPort, conf.contextPath.getOrElse(""))
    }
  }

  /** Open H2O Flow running in this client. */
  def openFlow(): Unit = openURI(flowURL())

  override def toString: String = {
    val basic =
      s"""
         |Sparkling Water Context:
         | * Sparkling Water Version: ${BuildInfo.SWVersion}
         | * H2O name: ${H2O.ARGS.name}
         | * cluster size: ${nodes.length}
         | * list of used nodes:
         |  (executorId, host, port)
         |  ------------------------
         |  ${nodes.mkString("\n  ")}
         |  ------------------------
         |
         |  Open H2O Flow in browser: ${flowURL()} (CMD + click in Mac OSX)
         |
    """.stripMargin
    val sparkYarnAppId = if (sparkContext.master.toLowerCase.startsWith("yarn")) {
      s"""
         | * Yarn App ID of Spark application: ${sparkContext.applicationId}
    """.stripMargin
    } else {
      ""
    }

    basic ++ sparkYarnAppId ++ backend.epilog
  }

  def isStopped(): Boolean = stopped

  /** Define implicits available via h2oContext.implicits._ */
  object implicits extends H2OContextImplicits with Serializable {
    protected override def hc: H2OContext = self
  }

  private def getSparklingWaterHeartbeatEvent(): SparklingWaterHeartbeatEvent = {
    val ping = RestApiUtils.getPingInfo(conf)
    val memoryInfo = ping.nodes.map(node => (node.ip_port, PrettyPrint.bytes(node.free_mem)))
    SparklingWaterHeartbeatEvent(ping.cloud_healthy, ping.cloud_uptime_millis, memoryInfo)
  }

  private def createHeartBeatEventThread(): Thread = {
    new Thread {
      setDaemon(true)

      override def run(): Unit = {
        while (!Thread.interrupted()) {
          try {
            val swHeartBeatInfo = getSparklingWaterHeartbeatEvent()
            if (conf.runsInExternalClusterMode) {
              if (!swHeartBeatInfo.cloudHealthy) {
                logError("H2O cluster not healthy!")
                if (conf.isKillOnUnhealthyClusterEnabled) {
                  logError("Stopping external H2O cluster as it is not healthy.")
                  if (H2OClientUtils.isH2OClientBased(conf)) {
                    H2OContext.this.stop(true)
                  } else {
                    H2OContext.this.stop(stopSparkContext = false, stopJvm = false, inShutdownHook = false)
                  }
                }
              }
            }
            Utils.postToListenerBus(swHeartBeatInfo)
          } catch {
            case cause: RestApiException =>
              if (H2OContext.get().isDefined) {
                H2OContext.get().head.stop(stopSparkContext = false, stopJvm = false, inShutdownHook = false)
              }
              if (!Utils.inShutdown()) {
                throw new H2OClusterNotReachableException(
                  s"""H2O cluster ${conf.h2oCluster.get + conf.contextPath
                       .getOrElse("")} - ${conf.cloudName.get} is not reachable,
                     |H2OContext has been closed! Please create a new H2OContext to a healthy and reachable (web enabled)
                     |H2O cluster.""".stripMargin,
                  cause)
              }
          }

          try {
            Thread.sleep(conf.backendHeartbeatInterval)
          } catch {
            case _: InterruptedException => Thread.currentThread.interrupt()
          }
        }
      }
    }
  }

  private def verifyExtensionJarIsAvailable(): Unit = {
    try {
      // Method getH2OLogLevel requires rest api endpoint which is part of the extensions jar. This call
      // will fail in case the extensions are not available.
      getH2OLogLevel()
    } catch {
      case _: Throwable =>
        throw new RuntimeException("External H2O cluster is missing extension jar on its classpath." +
          " Read the documentation to learn how to add the extension jar to the manually started external H2O cluster.")
    }
  }
}

object H2OContext extends Logging {
  private[H2OContext] def setInstantiatedContext(h2oContext: H2OContext): Unit = {
    synchronized {
      val ctx = instantiatedContext.get()
      if (ctx == null) {
        instantiatedContext.set(h2oContext)
      }
    }
  }

  private val instantiatedContext = new AtomicReference[H2OContext]()

  def get(): Option[H2OContext] = Option(instantiatedContext.get())

  def ensure(onError: => String = "H2OContext has to be running."): H2OContext =
    Option(instantiatedContext.get()) getOrElse {
      throw new RuntimeException(onError)
    }

  private def connectingToNewCluster(hc: H2OContext, newConf: H2OConf): Boolean = {
    val newCloudV3 = RestApiUtils.getClusterInfo(newConf)
    val sameNodes = hc.getH2ONodes().map(_.ipPort()).sameElements(newCloudV3.nodes.map(_.ip_port))
    !sameNodes
  }

  private def checkAndUpdateConf(conf: H2OConf): H2OConf = {
    if (conf.runsInExternalClusterMode) {
      ExternalH2OBackend.checkAndUpdateConf(conf)
    } else {
      InternalH2OBackend.checkAndUpdateConf(conf)
    }
  }

  /**
    * Get existing or create new H2OContext
    *
    * @param conf H2O configuration
    * @return H2O Context
    */
  def getOrCreate(conf: H2OConf): H2OContext = synchronized {
    if (H2OClientUtils.isH2OClientBased(conf)) {
      if (instantiatedContext.get() == null)
        if (H2O.API_PORT == 0) { // api port different than 0 means that client is already running
          val checkedConf = checkAndUpdateConf(conf)
          instantiatedContext.set(new H2OContext(checkedConf))
        } else {
          throw new IllegalArgumentException(
            """
              |H2O context hasn't been started successfully in the previous attempt and H2O client with previous configuration is already running.
              |Because of the current H2O limitation that it can't be restarted within a running JVM,
              |please restart your job or spark session and create new H2O context with new configuration.")
          """.stripMargin)
        }
    } else {
      val existingContext = instantiatedContext.get()
      if (existingContext != null) {
        val isExternalBackend = conf.runsInExternalClusterMode
        val startedManually = existingContext.getConf.isManualClusterStartUsed
        if (isExternalBackend && startedManually) {
          if (conf.h2oCluster.isEmpty) {
            throw new IllegalArgumentException("H2O cluster endpoint has to be specified!")
          }
          if (connectingToNewCluster(existingContext, conf)) {
            val checkedConf = checkAndUpdateConf(conf)
            instantiatedContext.set(new H2OContext(checkedConf))
            logWarning(s"Connecting to a new external H2O cluster : ${checkedConf.h2oCluster.get}")
          }
        }
      } else {
        val checkedConf = checkAndUpdateConf(conf)
        instantiatedContext.set(new H2OContext(checkedConf))
      }
    }
    instantiatedContext.get()
  }

  /**
    * Get existing or create new H2OContext
    *
    * @return H2O Context
    */
  def getOrCreate(): H2OContext = {
    getOrCreate(Option(instantiatedContext.get()).map(_.getConf).getOrElse(new H2OConf()))
  }

  private def logStartingInfo(conf: H2OConf): Unit = {
    logInfo("Sparkling Water version: " + BuildInfo.SWVersion)
    logInfo("Spark version: " + SparkSessionUtils.active.version)
    logInfo("Integrated H2O version: " + BuildInfo.H2OVersion)
    logInfo("The following Spark configuration is used: \n    " + conf.getAll.mkString("\n    "))
  }

  /** Checks whether version of provided Spark is the same as Spark's version designated for this Sparkling Water version.
    * We check for correct version in shell scripts and during the build but we need to do the check also in the code in cases when the user
    * executes for example spark-shell command with sparkling water assembly jar passed as --jars and initiates H2OContext.
    * (Because in that case no check for correct Spark version has been done so far.)
    */
  private def verifySparkVersion(): Unit = {
    val sc = SparkSessionUtils.active.sparkContext
    val runningOnCorrectSpark = sc.version.startsWith(BuildInfo.buildSparkMajorVersion)
    if (!runningOnCorrectSpark) {
      throw new WrongSparkVersion(
        s"You are trying to use Sparkling Water built for Spark ${BuildInfo.buildSparkMajorVersion}," +
          s" but your Spark is of version ${sc.version}. Please make sure to use correct Sparkling Water for your" +
          s" Spark and re-run the application.")
    }
  }
  // H2O Py/R Clients does not support certificate verification as well
  HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
    def verify(string: String, ssls: SSLSession) = true
  })
}
