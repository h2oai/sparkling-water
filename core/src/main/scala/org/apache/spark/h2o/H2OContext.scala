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

package org.apache.spark.h2o

import java.util.TimeZone
import java.util.concurrent.atomic.AtomicReference

import ai.h2o.sparkling.backend.converters.{DatasetConverter, SparkDataFrameConverter, SupportedRDD, SupportedRDDConverter}
import ai.h2o.sparkling.backend.external
import ai.h2o.sparkling.backend.external.{H2OContextUtils => _, _}
import ai.h2o.sparkling.backend.shared.{AzureDatabricksUtils, Converter, SharedBackendConf, SparklingBackend}
import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark._
import org.apache.spark.h2o.backends.internal.InternalH2OBackend
import org.apache.spark.h2o.ui._
import org.apache.spark.h2o.utils._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.ShutdownHookManager
import water._

import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.control.NoStackTrace

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
 * @param conf         H2O configuration
 */
abstract class H2OContext private(private val conf: H2OConf) extends Logging with H2OContextUtils {
  self =>
  val sparkSession = SparkSessionUtils.active
  val sparkContext = sparkSession.sparkContext
  private val backendHeartbeatThread = new Thread {
    override def run(): Unit = {
      while (!Thread.interrupted()) {
        try {
          val swHeartBeatInfo = getSparklingWaterHeartBeatEvent()
          if (conf.runsInExternalClusterMode) {
            if (!swHeartBeatInfo.cloudHealthy) {
              logError("External H2O cluster not healthy!")
              if (conf.isKillOnUnhealthyClusterEnabled) {
                logError("Stopping external H2O cluster as it is not healthy.")
                if (RestApiUtils.isRestAPIBased(Some(H2OContext.this))) {
                  H2OContext.this.stop(stopSparkContext = false, stopJvm = false, inShutdownHook = false)
                } else {
                  H2OContext.this.stop(true)
                }
              }
            }
          }
          sparkContext.listenerBus.post(swHeartBeatInfo)
        } catch {
          case cause: RestApiException =>
            H2OContext.get().head.stop(stopSparkContext = false, stopJvm = false, inShutdownHook = false)
            throw new H2OClusterNotReachableException(
              s"""External H2O cluster ${conf.h2oCluster.get + conf.contextPath.getOrElse("")} - ${conf.cloudName.get} is not reachable,
                 |H2OContext has been closed! Please create a new H2OContext to a healthy and reachable (web enabled)
                 |external H2O cluster.""".stripMargin, cause)
        }

        try {
          Thread.sleep(conf.backendHeartbeatInterval)
        } catch {
          case _: InterruptedException => Thread.currentThread.interrupt()
        }
      }
    }
  }
  backendHeartbeatThread.setDaemon(true)

  /** IP of H2O client */
  private var localClientIp: String = _
  /** REST port of H2O client */
  private var localClientPort: Int = _
  private var stopped = false
  /** Here the client means Python or R H2O client */
  private var clientConnected = false

  /** Used backend */
  protected val backend: SparklingBackend = if (conf.runsInExternalClusterMode) {
    new ExternalH2OBackend(this)
  } else {
    new InternalH2OBackend(this)
  }

  protected def getH2OEndpointIp(): String

  protected def getH2OEndpointPort(): Int

  protected def getFlowEndpoint(): String

  protected def getSelfNodeDesc(): Option[NodeDesc]

  def getH2ONodes(): Array[NodeDesc]

  protected def initBackend(): Unit

  private var shutdownHookRef: AnyRef = _

  /**
   * This method connects to external H2O cluster if spark.ext.h2o.externalClusterMode is set to true,
   * otherwise it creates new H2O cluster living in Spark
   */
  protected def init(): H2OContext = {
    logInfo("Sparkling Water version: " + BuildInfo.SWVersion)
    logInfo("Spark version: " + sparkContext.version)
    logInfo("Integrated H2O version: " + BuildInfo.H2OVersion)
    logInfo("The following Spark configuration is used: \n    " + conf.getAll.mkString("\n    "))
    if (!isRunningOnCorrectSpark(sparkContext)) {
      throw new WrongSparkVersion(s"You are trying to use Sparkling Water built for Spark ${BuildInfo.buildSparkMajorVersion}," +
        s" but your $$SPARK_HOME(=${sparkContext.getSparkHome().getOrElse("SPARK_HOME is not defined!")}) property" +
        s" points to Spark of version ${sparkContext.version}. Please ensure correct Spark is provided and" +
        s" re-run Sparkling Water.")
    }

    // Init the H2O Context in a way provided by used backend and return the list of H2O nodes in case of external
    // backend or list of spark executors on which H2O runs in case of internal backend
    initBackend()
    // The lowest priority used by Spark is 25 (removing temp dirs). We need to perform cleaning up of H2O
    // resources before Spark does as we run as embedded application inside the Spark
    shutdownHookRef = ShutdownHookManager.addShutdownHook(10) { () =>
      logWarning("Spark shutdown hook called, stopping H2OContext!")
      stop(stopSparkContext = false, stopJvm = false, inShutdownHook = true)
    }
    localClientIp = getH2OEndpointIp()
    localClientPort = getH2OEndpointPort()

    if (sparkContext.ui.isDefined) {
      SparkSpecificUtils.addSparklingWaterTab(sparkContext)
    }

    logInfo(s"Sparkling Water ${BuildInfo.SWVersion} started, status of context: $this ")
    updateUIAfterStart() // updates the spark UI
    backendHeartbeatThread.start() // start backend heartbeats

    val timezoneOption = sparkContext.conf.getOption("spark.sql.session.timeZone")
    if (timezoneOption.isDefined) {
      TimeZone.setDefault(TimeZone.getTimeZone(timezoneOption.get))
    }
    this
  }

  protected def getH2OBuildInfo(nodes: Array[NodeDesc]): H2OBuildInfo

  protected def getH2OClusterInfo(nodes: Array[NodeDesc]): H2OClusterInfo

  protected def getSparklingWaterHeartBeatEvent(): SparklingWaterHeartbeatEvent = backend.getSparklingWaterHeartbeatEvent

  private[this] def updateUIAfterStart(): Unit = {
    val nodes = getH2ONodes()
    val h2oBuildInfo = getH2OBuildInfo(nodes)
    val h2oClusterInfo = getH2OClusterInfo(nodes)
    val swPropertiesInfo = conf.getAll.filter(_._1.startsWith("spark.ext.h2o"))

    // Initial update
    val swHeartBeatEvent = getSparklingWaterHeartBeatEvent()
    SparkSessionUtils.active.sparkContext.listenerBus.post(swHeartBeatEvent)
    val listenerBus = SparkSessionUtils.active.sparkContext.listenerBus
    listenerBus.post(H2OContextStartedEvent(h2oClusterInfo, h2oBuildInfo, swPropertiesInfo))
  }


  /**
   * Return a copy of this H2OContext's configuration. The configuration ''cannot'' be changed at runtime.
   */
  def getConf: H2OConf = conf.clone()

  /**
   * The method transforms RDD to H2OFrame and returns String representation of its key.
   *
   * @param rdd Input RDD
   * @return String representation of H2O Frame Key
   */
  def asH2OFrameKeyString(rdd: SupportedRDD): String = asH2OFrameKeyString(rdd, None)

  def asH2OFrameKeyString(rdd: SupportedRDD, frameName: String): String = asH2OFrameKeyString(rdd, Some(frameName))

  def asH2OFrameKeyString(rdd: SupportedRDD, frameName: Option[String]): String = toH2OFrameKey(rdd, frameName).toString

  /** Transforms RDD[Supported type] to H2OFrame */
  def asH2OFrame(rdd: SupportedRDD): H2OFrame = asH2OFrame(rdd, None)

  def asH2OFrame(rdd: SupportedRDD, frameName: Option[String]): H2OFrame =
    withConversionDebugPrints(sparkContext, "SupportedRDD", {
      val converter = Converter.getConverter(conf)
      val key = SupportedRDDConverter.toH2OFrameKeyString(this, rdd, frameName, converter)
      new H2OFrame(DKV.getGet[Frame](key))
    })

  def asH2OFrame(rdd: SupportedRDD, frameName: String): H2OFrame = asH2OFrame(rdd, Option(frameName))


  /** Transforms RDD[Supported type] to H2OFrame key */
  def toH2OFrameKey(rdd: SupportedRDD): Key[_] = toH2OFrameKey(rdd, None)

  def toH2OFrameKey(rdd: SupportedRDD, frameName: Option[String]): Key[_] = asH2OFrame(rdd, frameName)._key

  def toH2OFrameKey(rdd: SupportedRDD, frameName: String): Key[_] = toH2OFrameKey(rdd, Option(frameName))

  /** Transform DataFrame to H2OFrame */
  def asH2OFrame(df: DataFrame): H2OFrame = asH2OFrame(df, None)

  def asH2OFrame(df: DataFrame, frameName: Option[String]): H2OFrame =
    withConversionDebugPrints(sparkContext, "DataFrame", SparkDataFrameConverter.toH2OFrame(this, df, frameName))

  def asH2OFrame(df: DataFrame, frameName: String): H2OFrame = asH2OFrame(df, Option(frameName))

  /**
   * The method transforms Spark DataFrame to H2OFrame and returns String representation of its key.
   *
   * @param df Input data frame
   * @return String representation of H2O Frame Key
   */
  def asH2OFrameKeyString(df: DataFrame): String = asH2OFrameKeyString(df, None)

  def asH2OFrameKeyString(df: DataFrame, frameName: String): String = asH2OFrameKeyString(df, Some(frameName))

  def asH2OFrameKeyString(df: DataFrame, frameName: Option[String]): String = toH2OFrameKey(df, frameName).toString

  /** Transform DataFrame to H2OFrame key */
  def toH2OFrameKey(df: DataFrame): Key[Frame] = toH2OFrameKey(df, None)

  def toH2OFrameKey(df: DataFrame, frameName: Option[String]): Key[Frame] = asH2OFrame(df, frameName)._key

  def toH2OFrameKey(df: DataFrame, frameName: String): Key[Frame] = toH2OFrameKey(df, Option(frameName))

  /** Transforms Dataset[Supported type] to H2OFrame */
  def asH2OFrame[T <: Product : TypeTag](ds: Dataset[T]): H2OFrame = asH2OFrame(ds, None)

  def asH2OFrame[T <: Product : TypeTag](ds: Dataset[T], frameName: Option[String]): H2OFrame =
    withConversionDebugPrints(sparkContext, "Dataset", DatasetConverter.toH2OFrame(this, ds, frameName))

  def asH2OFrame[T <: Product : TypeTag](ds: Dataset[T], frameName: String): H2OFrame = asH2OFrame(ds, Option(frameName))

  /** Transforms Dataset[Supported type] to H2OFrame key */
  def toH2OFrameKey[T <: Product : TypeTag](ds: Dataset[T]): Key[Frame] = toH2OFrameKey(ds, None)

  def toH2OFrameKey[T <: Product : TypeTag](ds: Dataset[T], frameName: Option[String]): Key[Frame] = asH2OFrame(ds, frameName)._key

  def toH2OFrameKey[T <: Product : TypeTag](ds: Dataset[T], frameName: String): Key[Frame] = toH2OFrameKey(ds, Option(frameName))

  /** Create a new H2OFrame based on existing Frame referenced by its key. */
  def asH2OFrame(s: String): H2OFrame = new H2OFrame(s)

  /** Create a new H2OFrame based on existing Frame */
  def asH2OFrame(fr: Frame): H2OFrame = new H2OFrame(fr)

  /** Convert given H2O frame into a Product RDD type
   *
   * Consider using asH2OFrame since asRDD has several limitations such as that asRDD can't be used in Spark REPL
   * in case we are RDD[T] where T is class defined in REPL. This is because class T is created as inner class
   * and we are not able to create instance of class T without outer scope - which is impossible to get.
   * */
  def asRDD[A <: Product : TypeTag : ClassTag](fr: H2OFrame): RDD[A] = SupportedRDDConverter.toRDD[A, H2OFrame](this, fr)

  /** A generic convert of Frame into Product RDD type
   *
   * Consider using asH2OFrame since asRDD has several limitations such as that asRDD can't be used in Spark REPL
   * in case we are RDD[T] where T is class defined in REPL. This is because class T is created as inner class
   * and we are not able to create instance of class T without outer scope - which is impossible to get.
   *
   * This code: hc.asRDD[PUBDEV458Type](rdd) will need to be call as hc.asRDD[PUBDEV458Type].apply(rdd)
   */
  def asRDD[A <: Product : TypeTag : ClassTag] = new {
    def apply[T <: Frame](fr: T): RDD[A] = SupportedRDDConverter.toRDD[A, T](H2OContext.this, fr)
  }

  /** Convert given H2O frame into DataFrame type */

  def asDataFrame[T <: Frame](fr: T, copyMetadata: Boolean = true): DataFrame = {
    SparkDataFrameConverter.toDataFrame(this, fr, copyMetadata)
  }

  def asDataFrame(s: String, copyMetadata: Boolean): DataFrame = {
    SparkDataFrameConverter.toDataFrame(this, new H2OFrame(s), copyMetadata)
  }

  def asDataFrame(s: String): DataFrame = asDataFrame(s, true)

  /** Returns location of REST API of H2O client */
  def h2oLocalClient = this.localClientIp + ":" + this.localClientPort + conf.contextPath.getOrElse("")

  /** Returns IP of H2O client */
  def h2oLocalClientIp = this.localClientIp

  /** Returns port where H2O REST API is exposed */
  def h2oLocalClientPort = this.localClientPort

  /** Set log level for the client running in driver */
  def setH2OClientLogLevel(level: String): Unit = LogUtil.setH2OClientLogLevel(level)

  /** Set log level for all H2O services running on executors */
  def setH2ONodeLogLevel(level: String): Unit = LogUtil.setH2ONodeLogLevel(level)

  /** Set H2O log level for the client and all executors */
  def setH2OLogLevel(level: String): Unit = {
    LogUtil.setH2OClientLogLevel(level)
    LogUtil.setH2ONodeLogLevel(level)
  }

  private def stop(stopSparkContext: Boolean, stopJvm: Boolean, inShutdownHook: Boolean): Unit = synchronized {
    if (!inShutdownHook) {
      ShutdownHookManager.removeShutdownHook(shutdownHookRef)
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
        if (RestApiUtils.isRestAPIBased(Some(this))) {
          RestApiUtils.shutdownCluster(conf)
        } else {
          H2O.orderlyShutdown(conf.externalBackendStopTimeout)
        }
      }
      H2OContext.instantiatedContext.set(null)
      stopped = true
      if (stopJvm && !RestApiUtils.isRestAPIBased(Some(this))) {
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
      s"${conf.getScheme()}://${getFlowEndpoint()}"
    }
  }

  /** Open H2O Flow running in this client. */
  def openFlow(): Unit = openURI(sparkContext, flowURL())

  override def toString: String = {
    val basic =
      s"""
         |Sparkling Water Context:
         | * Sparkling Water Version: ${BuildInfo.SWVersion}
         | * H2O name: ${H2O.ARGS.name}
         | * cluster size: ${getH2ONodes().length}
         | * list of used nodes:
         |  (executorId, host, port)
         |  ------------------------
         |  ${getH2ONodes().mkString("\n  ")}
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

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /** Define implicits available via h2oContext.implicits._ */
  object implicits extends H2OContextImplicits with Serializable {
    protected override def _h2oContext: H2OContext = self
  }

  // scalastyle:on

  def downloadH2OLogs(destinationDir: String, logContainer: String = "ZIP"): String
}

object H2OContext extends Logging {

  private class H2OContextClientBased(conf: H2OConf) extends H2OContext(conf) {

    /** Runtime list of active H2O nodes */
    protected var h2oNodes: Array[NodeDesc] = _

    override protected def getH2OEndpointIp(): String = {
      if (conf.ignoreSparkPublicDNS) {
        H2O.getIpPortString.split(":")(0)
      } else {
        sys.env.getOrElse("SPARK_PUBLIC_DNS", H2O.getIpPortString.split(":")(0))
      }
    }

    override protected def getH2OEndpointPort(): Int = H2O.API_PORT

    override protected def getSelfNodeDesc(): Option[NodeDesc] = Some(NodeDesc(H2O.SELF))

    override protected def getH2OClusterInfo(nodes: Array[NodeDesc]): H2OClusterInfo = {
      H2OClusterInfo(
        h2oLocalClient,
        H2O.CLOUD.healthy(),
        H2OSecurityManager.instance.securityEnabled,
        nodes.map(_.ipPort()),
        backend.backendUIInfo,
        H2O.START_TIME_MILLIS.get()
      )
    }

    override def getH2ONodes(): Array[NodeDesc] = h2oNodes

    override protected def initBackend(): Unit = {
      h2oNodes = backend.init(conf)
    }

    override protected def getH2OBuildInfo(nodes: Array[NodeDesc]): H2OBuildInfo = {
      H2OBuildInfo(
        H2O.ABV.projectVersion(),
        H2O.ABV.branchName(),
        H2O.ABV.lastCommitHash(),
        H2O.ABV.describe(),
        H2O.ABV.compiledBy(),
        H2O.ABV.compiledOn()
      )
    }

    override def downloadH2OLogs(destinationDir: String, logContainer: String = "ZIP"): String = {
      verifyLogContainer(logContainer)
      H2O.downloadLogs(destinationDir, logContainer).toString
    }

    override protected def getFlowEndpoint(): String = s"${getH2OEndpointIp()}:${getH2OEndpointPort()}${conf.contextPath.getOrElse("")}"
  }

  private class H2OContextRestAPIBased(conf: H2OConf) extends H2OContext(conf) with
    external.H2OContextUtils {
    private var flowIp: String = _
    private var flowPort: Int = _
    private var leaderNode: NodeDesc = _

    override protected def getH2OEndpointIp(): String = leaderNode.hostname

    override protected def getH2OEndpointPort(): Int = leaderNode.port

    override protected def getSelfNodeDesc(): Option[NodeDesc] = None

    override protected def getH2OClusterInfo(nodes: Array[NodeDesc]): H2OClusterInfo = {
      val cloudV3 = getClusterInfo(conf)
      H2OClusterInfo(
        s"$flowIp:$flowPort",
        cloudV3.cloud_healthy,
        cloudV3.internal_security_enabled,
        nodes.map(_.ipPort()),
        backend.backendUIInfo,
        cloudV3.cloud_uptime_millis)
    }

    override def getH2ONodes(): Array[NodeDesc] = getNodes(conf)

    override protected def initBackend(): Unit = {
      backend.init(conf)
      leaderNode = getLeaderNode(conf)
      val uri = ProxyStarter.startFlowProxy(conf)
      flowIp = uri.getHost
      flowPort = uri.getPort
    }

    override protected def getH2OBuildInfo(nodes: Array[NodeDesc]): H2OBuildInfo = {
      val cloudV3 = getClusterInfo(conf)
      H2OBuildInfo(
        cloudV3.version,
        cloudV3.branch_name,
        cloudV3.last_commit_hash,
        cloudV3.describe,
        cloudV3.compiled_by,
        cloudV3.compiled_on
      )
    }

    override def downloadH2OLogs(destinationDir: String, logContainer: String = "ZIP"): String = {
      verifyLogContainer(logContainer)
      downloadLogs(destinationDir, logContainer, conf)
    }

    override def asDataFrame(frameId: String, copyMetadata: Boolean): DataFrame = {
      val frame = ai.h2o.sparkling.frame.H2OFrame(frameId)
      SparkDataFrameConverter.toDataFrame(this, frame, copyMetadata)
    }

    def asRDD[A <: Product : TypeTag : ClassTag](fr: ai.h2o.sparkling.frame.H2OFrame): org.apache.spark.rdd.RDD[A] = {
      SupportedRDDConverter.toRDD[A](this, fr)
    }

    override def asH2OFrameKeyString(df: DataFrame, frameName: Option[String]): String = {
      SparkDataFrameConverter.toH2OFrameKeyString(this, df, frameName, ExternalBackendConverter)
    }

    override def asH2OFrameKeyString(rdd: SupportedRDD, frameName: Option[String]): String = {
      SupportedRDDConverter.toH2OFrameKeyString(this, rdd, frameName, ExternalBackendConverter)
    }

    override protected def getFlowEndpoint(): String = s"$flowIp:$flowPort${conf.contextPath.getOrElse("")}"

  }

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
    val isRestApiBasedClient = conf.getBoolean(SharedBackendConf.PROP_REST_API_BASED_CLIENT._1,
      SharedBackendConf.PROP_REST_API_BASED_CLIENT._2)
    val isExternalBackend = conf.runsInExternalClusterMode
    if (isExternalBackend && isRestApiBasedClient) {
      val existingContext = instantiatedContext.get()
      if (existingContext != null) {
        val startedManually = existingContext.conf.isManualClusterStartUsed
        if (startedManually) {
          if (conf.h2oCluster.isEmpty) {
            throw new IllegalArgumentException("H2O Cluster endpoint has to be specified!")
          }
          if (connectingToNewCluster(existingContext, conf)) {
            val checkedConf = checkAndUpdateConf(conf)
            instantiatedContext.set(new H2OContextRestAPIBased(checkedConf).init())
            logWarning(s"Connecting to a new external H2O cluster : ${checkedConf.h2oCluster.get}")
          }
        }

      } else {
        val checkedConf = checkAndUpdateConf(conf)
        instantiatedContext.set(new H2OContextRestAPIBased(checkedConf).init())
      }
    } else {
      if (instantiatedContext.get() == null)
        if (H2O.API_PORT == 0) { // api port different than 0 means that client is already running
          val checkedConf = checkAndUpdateConf(conf)
          instantiatedContext.set(new H2OContextClientBased(checkedConf).init())
        } else {
          throw new IllegalArgumentException(
            """
              |H2O context hasn't been started successfully in the previous attempt and H2O client with previous configuration is already running.
              |Because of the current H2O limitation that it can't be restarted within a running JVM,
              |please restart your job or spark session and create new H2O context with new configuration.")
          """.stripMargin)
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

  @DeprecatedMethod("getOrCreate(conf: H2OConf)", "3.32")
  def getOrCreate(sparkSession: SparkSession, conf: H2OConf): H2OContext = {
    getOrCreate(conf)
  }

  @DeprecatedMethod("getOrCreate(conf: H2OConf)", "3.32")
  def getOrCreate(sc: SparkContext, conf: H2OConf): H2OContext = {
    getOrCreate(conf)
  }

  @DeprecatedMethod("getOrCreate()", "3.32")
  def getOrCreate(sparkSession: SparkSession): H2OContext = {
    getOrCreate()
  }

  @DeprecatedMethod("getOrCreate()", "3.32")
  def getOrCreate(sc: SparkContext): H2OContext = {
    getOrCreate()
  }
}

class WrongSparkVersion(msg: String) extends Exception(msg) with NoStackTrace
