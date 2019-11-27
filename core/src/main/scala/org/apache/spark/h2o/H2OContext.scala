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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark._
import org.apache.spark.h2o.backends.external.ExternalH2OBackend
import org.apache.spark.h2o.backends.internal.InternalH2OBackend
import org.apache.spark.h2o.backends.{SharedBackendConf, SparklingBackend}
import org.apache.spark.h2o.converters._
import org.apache.spark.h2o.ui._
import org.apache.spark.h2o.utils._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import water._
import water.util.PrettyPrint
import org.apache.spark.util.ShutdownHookManager

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
  * @param sparkSession Spark Session
  * @param conf         H2O configuration
  */
abstract class H2OContext private(val sparkSession: SparkSession, private val conf: H2OConf) extends Logging with H2OContextUtils {
  self =>
  val sparkContext = sparkSession.sparkContext
  private val uiUpdateThread = new Thread {
    override def run(): Unit = {
      while (!Thread.interrupted()) {
        val swHeartBeatInfo = getSparklingWaterHeartBeatEvent()
        sparkContext.listenerBus.post(swHeartBeatInfo)
        try {
          Thread.sleep(conf.uiUpdateInterval)
        } catch {
          case _: InterruptedException => Thread.currentThread.interrupt()
        }
      }
    }
  }
  uiUpdateThread.setDaemon(true)

  /** IP of H2O client */
  private var localClientIp: String = _
  /** REST port of H2O client */
  private var localClientPort: Int = _
  private var stopped = false
  /** Here the client means Python or R H2O client */
  private var clientConnected = false

  /** Used backend */
  val backend: SparklingBackend = if (conf.runsInExternalClusterMode) {
    new ExternalH2OBackend(this)
  } else {
    new InternalH2OBackend(this)
  }

  protected def getFlowIp(): String

  protected def getFlowPort(): Int

  protected def getSelfNodeDesc(): Option[NodeDesc]

  def getH2ONodes(): Array[NodeDesc]

  protected def initBackend(): Unit

  /**
    * This method connects to external H2O cluster if spark.ext.h2o.externalClusterMode is set to true,
    * otherwise it creates new H2O cluster living in Spark
    */
  def init(): H2OContext = {
    // The lowest priority used by Spark is 25 (removing temp dirs). We need to perform cleaning up of H2O
    // resources before Spark does as we run as embedded application inside the Spark
    ShutdownHookManager.addShutdownHook(10) { () =>
      logWarning("Spark shutdown hook called, stopping H2OContext!")
      stop(stopSparkContext = false, stopJvm = false)
    }
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

    // Check that we have Duke library available. It is available when using the assembly JAR at all cases, however,
    // when Sparkling Water is used via --packages, it is not correctly resolved and needs to be set manually.
    try {
      Class.forName("no.priv.garshol.duke.Comparator")
    } catch {
      case _: ClassNotFoundException => throw new RuntimeException(s"When using the Sparkling Water as Spark package " +
        s"via --packages option, the 'no.priv.garshol.duke:duke:1.2' dependency has to be specified explicitly due to" +
        s" a bug in Spark dependency resolution.")
    }

    // Init the H2O Context in a way provided by used backend and return the list of H2O nodes in case of external
    // backend or list of spark executors on which H2O runs in case of internal backend
    initBackend()
    localClientIp = getFlowIp()

    localClientPort = getFlowPort()

    SparkSpecificUtils.addSparklingWaterTab(sparkContext)

    logInfo(s"Sparkling Water ${BuildInfo.SWVersion} started, status of context: $this ")
    updateUIAfterStart() // updates the spark UI
    uiUpdateThread.start() // start periodical updates of the UI
    this
  }

  protected def getH2OBuildInfo(nodes: Array[NodeDesc]): H2OBuildInfo

  protected def getH2OClusterInfo(nodes: Array[NodeDesc]): H2OClusterInfo

  protected def getSparklingWaterHeartBeatEvent(): SparklingWaterHeartbeatEvent

  private[this] def updateUIAfterStart(): Unit = {
    val nodes = getH2ONodes()
    val h2oBuildInfo = getH2OBuildInfo(nodes)
    val h2oClusterInfo = getH2OClusterInfo(nodes)
    val swPropertiesInfo = conf.getAll.filter(_._1.startsWith("spark.ext.h2o"))

    // Initial update
    val swHeartBeatEvent = getSparklingWaterHeartBeatEvent()
    sparkSession.sparkContext.listenerBus.post(swHeartBeatEvent)
    sparkSession.sparkContext.listenerBus.post(H2OContextStartedEvent(h2oClusterInfo, h2oBuildInfo, swPropertiesInfo))
  }


  /**
    * Return a copy of this H2OContext's configuration. The configuration ''cannot'' be changed at runtime.
    */
  def getConf: H2OConf = conf.clone()

  /** Transforms RDD[Supported type] to H2OFrame */
  def asH2OFrame(rdd: SupportedRDD): H2OFrame = asH2OFrame(rdd, None)

  def asH2OFrame(rdd: SupportedRDD, frameName: Option[String]): H2OFrame =
    withConversionDebugPrints(sparkContext, "SupportedRDD", SupportedRDDConverter.toH2OFrame(this, rdd, frameName))

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
    def apply[T <: Frame](fr: T): H2ORDD[A, T] = SupportedRDDConverter.toRDD[A, T](H2OContext.this, fr)
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

  private def stop(stopSparkContext: Boolean, stopJvm: Boolean): Unit = synchronized {
    if (!stopped) {
      uiUpdateThread.interrupt()
      if (stopSparkContext) {
        sparkContext.stop()
      }
      // H2O cluster is managed by the user in case of manual start of external backend.
      if (!(conf.isManualClusterStartUsed && conf.runsInExternalClusterMode)) {
        H2O.orderlyShutdown()
      }
      if (stopJvm && conf.get("spark.ext.h2o.rest.api.based.client", "false") == "false") {
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
    stop(stopSparkContext, stopJvm = true)
  }

  def flowURL(): String = {
    if (AzureDatabricksUtils.isRunningOnAzureDatabricks(conf)) {
      AzureDatabricksUtils.flowURL(conf)
    } else if (conf.clientFlowBaseurlOverride.isDefined) {
      conf.clientFlowBaseurlOverride.get + conf.contextPath.getOrElse("")
    } else {
      s"${conf.getScheme()}://$h2oLocalClient"
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

  private class H2OContextClientBased(spark: SparkSession, conf: H2OConf) extends H2OContext(spark, conf) {

    /** Runtime list of active H2O nodes */
    protected var h2oNodes: Array[NodeDesc] = _

    override protected def getFlowIp(): String = {
      if (conf.ignoreSparkPublicDNS) {
        H2O.getIpPortString.split(":")(0)
      } else {
        sys.env.getOrElse("SPARK_PUBLIC_DNS", H2O.getIpPortString.split(":")(0))
      }
    }

    override protected def getFlowPort(): Int = H2O.API_PORT

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

    override protected def getSparklingWaterHeartBeatEvent(): SparklingWaterHeartbeatEvent = {
      val members = H2O.CLOUD.members() ++ Array(H2O.SELF)
      val memoryInfo = members.map(node => (node.getIpPortString, PrettyPrint.bytes(node._heartbeat.get_free_mem())))
      SparklingWaterHeartbeatEvent(H2O.CLOUD.healthy(), System.currentTimeMillis(), memoryInfo)
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

  }

  private class H2OContextRestAPIBased(spark: SparkSession, conf: H2OConf) extends H2OContext(spark, conf) with H2OContextRestAPIUtils {
    private var flowIp: String = _
    private var flowPort: Int = _
    override protected def getFlowIp(): String = flowIp

    override protected def getFlowPort(): Int = flowPort

    override protected def getSelfNodeDesc(): Option[NodeDesc] = None

    override protected def getH2OClusterInfo(nodes: Array[NodeDesc]): H2OClusterInfo = {
      val cloudV3 = getCloudInfo(conf)
      H2OClusterInfo(
        s"$flowIp:$flowPort",
        cloudV3.cloud_healthy,
        cloudV3.internal_security_enabled,
        nodes.map(_.ipPort()),
        backend.backendUIInfo,
        cloudV3.cloud_uptime_millis)
    }

    override protected def getSparklingWaterHeartBeatEvent(): SparklingWaterHeartbeatEvent = {
      try {
        val ping = getPingInfo(conf)
        val memoryInfo = ping.nodes.map(node => (node.ip_port, PrettyPrint.bytes(node.free_mem)))
        SparklingWaterHeartbeatEvent(ping.cloud_healthy, ping.cloud_uptime_millis, memoryInfo)
      } catch {
        case e: H2OClusterNodeNotReachableException =>
          H2OContext.get().head.stop()
          throw new H2OClusterNodeNotReachableException(
            s"""External H2O cluster ${conf.h2oCluster.get + conf.contextPath.getOrElse("")} - ${conf.cloudName.get} is not reachable,
               |H2OContext has been closed.
               |Please create a new H2OContext to a healthy and reachable (web enabled) external H2O cluster.""".stripMargin, e.getCause)
      }
    }

    override def getH2ONodes(): Array[NodeDesc] = getNodes(conf)

    override protected def initBackend(): Unit = {
      backend.init(conf)
      val uri = startFlowProxy(conf)
      flowIp = uri.getHost
      flowPort = uri.getPort
    }

    override protected def getH2OBuildInfo(nodes: Array[NodeDesc]): H2OBuildInfo = {
      val cloudV3 = getCloudInfo(conf)
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
      H2OContextRestAPIUtils.downloadLogs(destinationDir, logContainer, conf)
    }

    override def asDataFrame(frameId: String, copyMetadata: Boolean): DataFrame = {
      val frame = getFrame(conf, frameId)
      SparkDataFrameConverter.toDataFrame(this, frame, copyMetadata)
    }

    def asRDD[A <: Product : TypeTag : ClassTag](fr: ai.h2o.sparkling.frame.H2OFrame): org.apache.spark.rdd.RDD[A] = {
      SupportedRDDConverter.toRDD[A](this, fr)
    }
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

  /**
    * Tries to get existing H2O Context. If it is not there, ok.
    * Note that this method has to be here because otherwise ScalaCodeHandlerSuite will fail in one of the tests.
    * If you want to throw an exception when the context is missing, use ensure()
    * If you want to create the context if it is not missing, use getOrCreate() (if you can).
    *
    * @return Option containing H2O Context or None
    */
  def get(): Option[H2OContext] = Option(instantiatedContext.get())

  def ensure(onError: => String = "H2OContext has to be started in order to save/load data using H2O Data source."): H2OContext =
    Option(instantiatedContext.get()) getOrElse {
      throw new RuntimeException(onError)
    }

  private def connectingToNewCluster(hc: H2OContext, newConf: H2OConf): Boolean = {
    val newCloudV3 = H2OContextRestAPIUtils.getCloudInfo(newConf)
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
    * Get existing or create new H2OContext based on provided H2O configuration
    *
    * @param sparkSession Spark Session
    * @param conf         H2O configuration
    * @return H2O Context
    */
  def getOrCreate(sparkSession: SparkSession, conf: H2OConf): H2OContext = synchronized {
    val checkedConf = checkAndUpdateConf(conf)
    val isRestApiBasedClient = conf.getBoolean(SharedBackendConf.PROP_REST_API_BASED_CLIENT._1,
      SharedBackendConf.PROP_REST_API_BASED_CLIENT._2)
    val isExternalBackend = conf.runsInExternalClusterMode
    if (isExternalBackend && isRestApiBasedClient) {
      if (instantiatedContext.get() != null) {
        if (connectingToNewCluster(instantiatedContext.get(), checkedConf)) {
          instantiatedContext.set(new H2OContextRestAPIBased(sparkSession, checkedConf).init())
          logWarning(s"Connecting to a new external H2O cluster : ${checkedConf.h2oCluster.get}")
        }
      } else {
        instantiatedContext.set(new H2OContextRestAPIBased(sparkSession, checkedConf).init())
      }
    } else {
      if (instantiatedContext.get() == null)
        if (H2O.API_PORT == 0) { // api port different than 0 means that client is already running
          instantiatedContext.set(new H2OContextClientBased(sparkSession, checkedConf).init())
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

  def getOrCreate(sc: SparkContext, conf: H2OConf): H2OContext = {
    logWarning("Method H2OContext.getOrCreate with an argument of type SparkContext is deprecated and " +
      "parameter of type SparkSession is preferred.")
    getOrCreate(SparkSession.builder().sparkContext(sc).getOrCreate(), conf)
  }

  /**
    * Get existing or create new H2OContext based on provided H2O configuration. It searches the configuration
    * properties passed to Sparkling Water and based on them starts H2O Context. If the values are not found, the default
    * values are used in most of the cases. The default cluster mode is internal, ie. spark.ext.h2o.external.cluster.mode=false
    *
    * @param sparkSession Spark Session
    * @return H2O Context
    */
  def getOrCreate(sparkSession: SparkSession): H2OContext = {
    getOrCreate(sparkSession, Option(instantiatedContext.get()).map(_.getConf).getOrElse(new H2OConf(sparkSession)))
  }

  def getOrCreate(sc: SparkContext): H2OContext = {
    logWarning("Method H2OContext.getOrCreate with an argument of type SparkContext is deprecated and " +
      "parameter of type SparkSession is preferred.")
    val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
    getOrCreate(spark)
  }

  /** Global cleanup on H2OContext.stop call */
  private def stop(context: H2OContext): Unit = {
    instantiatedContext.set(null)
  }

}

class WrongSparkVersion(msg: String) extends Exception(msg) with NoStackTrace
