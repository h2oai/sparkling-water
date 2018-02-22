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

import java.net.URI
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.h2o.backends.SparklingBackend
import org.apache.spark.h2o.backends.external.ExternalH2OBackend
import org.apache.spark.h2o.backends.internal.InternalH2OBackend
import org.apache.spark.h2o.converters._
import org.apache.spark.h2o.ui._
import org.apache.spark.h2o.utils.{H2OContextUtils, LogUtil, NodeDesc}
import org.apache.spark.internal.Logging
import org.apache.spark.network.Security
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import water._
import water.util.{Log, LogBridge}

import scala.collection.mutable
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
class H2OContext private(val sparkSession: SparkSession, conf: H2OConf) extends Logging with H2OContextUtils {
  self =>
  val announcementService = AnnouncementServiceFactory.create(conf)
  val sparkContext = sparkSession.sparkContext
  val sparklingWaterListener = new SparklingWaterListener(sparkContext.conf)
  val uiUpdateThread = new H2ORuntimeInfoUIThread(sparkContext, conf)
  /** IP of H2O client */
  private var localClientIp: String = _
  /** REST port of H2O client */
  private var localClientPort: Int = _
  /** Runtime list of active H2O nodes */
  private val h2oNodes = mutable.ArrayBuffer.empty[NodeDesc]
  private var stopped = false

  /** Used backend */
  val backend: SparklingBackend = if (conf.runsInExternalClusterMode) {
    new ExternalH2OBackend(this)
  } else {
    new InternalH2OBackend(this)
  }


  // Check Spark and H2O environment for general arguments independent on backend used and
  // also with regards to used backend and store the fix the state of prepared configuration
  // so it can't be changed anymore
  /** H2O and Spark configuration */
  val _conf: H2OConf = backend.checkAndUpdateConf(conf).clone()

  /**
    * This method connects to external H2O cluster if spark.ext.h2o.externalClusterMode is set to true,
    * otherwise it creates new H2O cluster living in Spark
    */
  def init(): H2OContext = {
    // Use H2O's logging as H2O info log level is default
    Log.info("Sparkling Water version: " + BuildInfo.SWVersion)
    Log.info("Spark version: " + sparkContext.version)
    Log.info("Integrated H2O version: " + BuildInfo.H2OVersion)
    Log.info("The following Spark configuration is used: \n    " + _conf.getAll.mkString("\n    "))
    Log.info("")
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

    if (conf.isInternalSecureConnectionsEnabled) {
      Security.enableSSL(sparkSession, conf)
    }

    sparkContext.addSparkListener(sparklingWaterListener)
    // Init the H2O Context in a way provided by used backend and return the list of H2O nodes in case of external
    // backend or list of spark executors on which H2O runs in case of internal backend
    val nodes = backend.init()
    // Fill information about H2O client and H2O nodes in the cluster
    h2oNodes.append(nodes: _*)
    localClientIp = sys.env.getOrElse("SPARK_PUBLIC_DNS", sparkContext.env.rpcEnv.address.host)
    localClientPort = H2O.API_PORT
    // Register UI, but not in Databricks as Databricks is not using standard Spark UI API
    if (conf.getBoolean("spark.ui.enabled", true) && !isRunningOnDatabricks()) {
      new SparklingWaterUITab(sparklingWaterListener, sparkContext.ui.get)
    }

    // Force initialization of H2O logs so flow and other dependant tools have logs available from the start
    val level = LogBridge.getH2OLogLevel()
    LogBridge.setH2OLogLevel(Log.TRACE) // just temporarily, set Trace Level so we can
    // be sure the Log.trace will initialize the logging and creates the log files
    Log.trace("H2OContext initialized") // force creation of log files
    LogBridge.setH2OLogLevel(level) // set the level back on the level user want's to use

    logInfo("Sparkling Water started, status of context: " + this)
    // Announce Flow UI location
    announcementService.announce(FlowLocationAnnouncement(H2O.ARGS.name, "http", localClientIp, localClientPort))
    updateUIAfterStart() // updates the spark UI
    uiUpdateThread.start() // start periodical updates of the UI

    this
  }

  private[this] def updateUIAfterStart(): Unit = {
    val h2oBuildInfo = H2OBuildInfo(
      H2O.ABV.projectVersion(),
      H2O.ABV.branchName(),
      H2O.ABV.lastCommitHash(),
      H2O.ABV.describe(),
      H2O.ABV.compiledBy(),
      H2O.ABV.compiledOn()
    )
    val h2oCloudInfo = H2OCloudInfo(
      h2oLocalClient,
      H2O.CLOUD.healthy(),
      H2OSecurityManager.instance.securityEnabled,
      H2O.CLOUD.members().map(node => node.getIpPortString),
      backend.backendUIInfo,
      H2O.START_TIME_MILLIS.get()
    )

    val swPropertiesInfo = _conf.getAll.filter(_._1.startsWith("spark.ext.h2o"))

    sparkSession.sparkContext.listenerBus.post(SparkListenerH2OStart(
      h2oCloudInfo,
      h2oBuildInfo,
      swPropertiesInfo
    ))
  }


  /**
    * Return a copy of this H2OContext's configuration. The configuration ''cannot'' be changed at runtime.
    */
  def getConf: H2OConf = _conf.clone()

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
  def asDataFrame[T <: Frame](fr: T, copyMetadata: Boolean = true)(implicit sqlContext: SQLContext): DataFrame =
    SparkDataFrameConverter.toDataFrame(this, fr, copyMetadata)

  def asDataFrame(s: String, copyMetadata: Boolean)(implicit sqlContext: SQLContext): DataFrame =
    SparkDataFrameConverter.toDataFrame(this, new H2OFrame(s), copyMetadata)

  /** Returns location of REST API of H2O client */
  def h2oLocalClient = this.localClientIp + ":" + this.localClientPort

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

  /** Stops H2O context.
    *
    * @param stopSparkContext stop also spark context
    */
  def stop(stopSparkContext: Boolean = false): Unit = synchronized {
    if (!stopped) {
      announcementService.shutdown
      uiUpdateThread.interrupt()
      backend.stop(stopSparkContext)
      H2OContext.stop(this)
      stopped = true
    } else {
      logWarning("H2OContext is already stopped, this call has no effect anymore")
    }
  }

  /** Open H2O Flow running in this client. */
  def openFlow(): Unit = openURI(sparkContext, s"http://$h2oLocalClient")

  override def toString: String = {
    val basic =
      s"""
         |Sparkling Water Context:
         | * H2O name: ${H2O.ARGS.name}
         | * cluster size: ${h2oNodes.size}
         | * list of used nodes:
         |  (executorId, host, port)
         |  ------------------------
         |  ${h2oNodes.mkString("\n  ")}
         |  ------------------------
         |
         |  Open H2O Flow in browser: http://$h2oLocalClient (CMD + click in Mac OSX)
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

  /**
    * Get existing or create new H2OContext based on provided H2O configuration
    *
    * @param sparkSession Spark Session
    * @param conf         H2O configuration
    * @return H2O Context
    */
  def getOrCreate(sparkSession: SparkSession, conf: H2OConf): H2OContext = synchronized {
    if (instantiatedContext.get() == null) {
      if (H2O.API_PORT == 0) { // api port different than 0 means that client is already running
        instantiatedContext.set(new H2OContext(sparkSession, conf).init())
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
    getOrCreate(sparkSession, new H2OConf(sparkSession))
  }

  def getOrCreate(sc: SparkContext): H2OContext = {
    logWarning("Method H2OContext.getOrCreate with an argument of type SparkContext is deprecated and " +
      "parameter of type SparkSession is preferred.")
    val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
    getOrCreate(spark, new H2OConf(spark))
  }

  /** Global cleanup on H2OContext.stop call */
  private def stop(context: H2OContext): Unit = {
    instantiatedContext.set(null)
  }

}

class WrongSparkVersion(msg: String) extends Exception(msg) with NoStackTrace
