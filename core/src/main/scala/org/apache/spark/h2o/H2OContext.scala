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
import org.apache.spark.h2o.backends.SparklingBackend
import org.apache.spark.h2o.backends.external.ExternalH2OBackend
import org.apache.spark.h2o.backends.internal.InternalH2OBackend
import org.apache.spark.h2o.converters._
import org.apache.spark.h2o.utils.{H2OContextUtils, LogUtil, NodeDesc}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import water._

import scala.collection.mutable
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.control.NoStackTrace

/**
  * Main entry point for Sparkling Water functionality. H2O Context represents connection to H2O cluster and allows as
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
  * @param sparkContext Spark Context
  * @param conf H2O configuration
  */
class H2OContext private (val sparkContext: SparkContext, conf: H2OConf) extends Logging with H2OContextUtils {
  self =>

  val sqlc: SQLContext = SparkSession.builder().getOrCreate().sqlContext

  /** IP of H2O client */
  private var localClientIp: String = _
  /** REST port of H2O client */
  private var localClientPort: Int = _
  /** Runtime list of active H2O nodes */
  private val h2oNodes = mutable.ArrayBuffer.empty[NodeDesc]


  /** Used backend */
  private val backend: SparklingBackend = if(conf.runsInExternalClusterMode){
    new ExternalH2OBackend(this)
  }else{
    new InternalH2OBackend(this)
  }


  // Check Spark and H2O environment for general arguments independent on backend used and
  // also with regards to used backend and store the fix the state of prepared configuration
  // so it can't be changed anymore
  /** H2O and Spark configuration */
  val _conf = backend.checkAndUpdateConf(conf).clone()

  /**
    * This method connects to external H2O cluster if spark.ext.h2o.externalClusterMode is set to true,
    * otherwise it creates new H2O cluster living in Spark
    */
  def init(): H2OContext = {
    if (!isRunningOnCorrectSpark(sparkContext)) {
      throw new WrongSparkVersion(s"You are trying to use Sparkling Water built for Spark $buildSparkMajorVersion," +
        s" but your $$SPARK_HOME(=${sparkContext.getSparkHome().getOrElse("SPARK_HOME is not defined!")}) property" +
        s" points to Spark of version ${sparkContext.version}. Please ensure correct Spark is provided and" +
        s" re-run Sparkling Water.")
    }

    // Init the H2O Context in a way provided by used backend and return the list of H2O nodes in case of external
    // backend or list of spark executors on which H2O runs in case of internal backend
    val nodes = backend.init()

    // Fill information about H2O client and H2O nodes in the cluster
    h2oNodes.append(nodes:_*)
    localClientIp = H2O.SELF_ADDRESS.getHostAddress
    localClientPort = H2O.API_PORT
    logInfo("Sparkling Water started, status of context: " + this)
    this
  }

  /**
    * Return a copy of this H2OContext's configuration. The configuration ''cannot'' be changed at runtime.
    */
  def getConf: H2OConf = _conf.clone()

  /** Transforms RDD[Supported type] to H2OFrame */
  def asH2OFrame(rdd: SupportedRDD): H2OFrame = asH2OFrame(rdd, None)
  def asH2OFrame(rdd: SupportedRDD, frameName: Option[String]): H2OFrame =
    SupportedRDDConverter.toH2OFrame(this, rdd, frameName)
  def asH2OFrame(rdd: SupportedRDD, frameName: String): H2OFrame = asH2OFrame(rdd, Option(frameName))


  /** Transforms RDD[Supported type] to H2OFrame key */
  def toH2OFrameKey(rdd: SupportedRDD): Key[_] = toH2OFrameKey(rdd, None)
  def toH2OFrameKey(rdd: SupportedRDD, frameName: Option[String]): Key[_] = asH2OFrame(rdd, frameName)._key
  def toH2OFrameKey(rdd: SupportedRDD, frameName: String): Key[_] = toH2OFrameKey(rdd, Option(frameName))

  /** Transform DataFrame to H2OFrame */
  def asH2OFrame(df: DataFrame): H2OFrame = asH2OFrame(df, None)
  def asH2OFrame(df: DataFrame, frameName: Option[String]): H2OFrame =
    SparkDataFrameConverter.toH2OFrame(this, df, frameName)
  def asH2OFrame(df: DataFrame, frameName: String): H2OFrame = asH2OFrame(df, Option(frameName))

  /** Transform DataFrame to H2OFrame key */
  def toH2OFrameKey(df : DataFrame): Key[Frame] = toH2OFrameKey(df, None)
  def toH2OFrameKey(df : DataFrame, frameName: Option[String]): Key[Frame] = asH2OFrame(df, frameName)._key
  def toH2OFrameKey(df : DataFrame, frameName: String): Key[Frame] = toH2OFrameKey(df, Option(frameName))

  /** Transforms Dataset[Supported type] to H2OFrame */
  def asH2OFrame[T<: Product : TypeTag](ds: Dataset[T]): H2OFrame = asH2OFrame(ds, None)
  def asH2OFrame[T<: Product : TypeTag](ds: Dataset[T], frameName: Option[String]): H2OFrame =
    DatasetConverter.toH2OFrame(this, ds,frameName)
  def asH2OFrame[T<: Product : TypeTag](ds: Dataset[T], frameName: String): H2OFrame = asH2OFrame(ds, Option(frameName))

  /** Transforms Dataset[Supported type] to H2OFrame key */
  def toH2OFrameKey[T<: Product : TypeTag](ds: Dataset[T]): Key[Frame] = toH2OFrameKey(ds, None)
  def toH2OFrameKey[T<: Product : TypeTag](ds: Dataset[T], frameName: Option[String]): Key[Frame] = asH2OFrame(ds, frameName)._key
  def toH2OFrameKey[T<: Product : TypeTag](ds: Dataset[T], frameName: String): Key[Frame] = toH2OFrameKey(ds, Option(frameName))

  /** Create a new H2OFrame based on existing Frame referenced by its key.*/
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
    * @param stopSparkContext  stop also spark context
    */
  def stop(stopSparkContext: Boolean = false): Unit = backend.stop(stopSparkContext)

  /** Open H2O Flow running in this client. */
  def openFlow(): Unit = openURI(sparkContext, s"http://$h2oLocalClient")

  /** Open Spark task manager. */
  //def openSparkUI(): Unit = sparkUI.foreach(openURI(_))

  override def toString: String = {
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
    """.stripMargin
  }

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /** Define implicits available via h2oContext.implicits._*/
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
    * @param sc Spark Context
    * @param conf H2O configuration
    * @return H2O Context
    */
  def getOrCreate(sc: SparkContext, conf: H2OConf): H2OContext = synchronized {
    if (instantiatedContext.get() == null) {
      if (H2O.API_PORT == 0) { // api port different than 0 means that client is already running
        instantiatedContext.set(new H2OContext(sc, conf).init())
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
    * Get existing or create new H2OContext based on provided H2O configuration. It searches the configuration
    * properties passed to Sparkling Water and based on them starts H2O Context. If the values are not found, the default
    * values are used in most of the cases. The default cluster mode is internal, ie. spark.ext.h2o.external.cluster.mode=false
    *
    * @param sc Spark Context
    * @return H2O Context
    */
  def getOrCreate(sc: SparkContext): H2OContext = {
    getOrCreate(sc, new H2OConf(sc))
  }

}

class WrongSparkVersion(msg: String) extends Exception(msg) with NoStackTrace

