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
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.h2o.H2OContextUtils._
import org.apache.spark.h2o.H2OTypeUtils._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.{H2ORDD, H2OSchemaRDD}
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import water._
import water.api.DataFrames.DataFramesHandler
import water.api.H2OFrames.H2OFramesHandler
import water.api.RDDs.RDDsHandler
import water.api._
import water.api.scalaInt.ScalaCodeHandler
import water.parser.BufferedString

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Random
import scala.util.control.NoStackTrace

/**
 * Simple H2O context motivated by SQLContext.
 *
 * It provides implicit conversion from RDD -> H2OLikeRDD and back.
 */
class H2OContext (@transient val sparkContext: SparkContext) extends {
    val sparkConf = sparkContext.getConf
  } with org.apache.spark.Logging
  with H2OConf
  with Serializable {
  self =>

  /** Supports call from java environments. */
  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  /** Runtime list of active H2O nodes */
  private val h2oNodes = mutable.ArrayBuffer.empty[NodeDesc]

  /** IP of H2O client */
  private var localClientIp: String = _
  /** REST port of H2O client */
  private var localClientPort: Int = _

  /** Transforms RDD[Supported type] to H2OFrame */
  def asH2OFrame(rdd: SupportedRDD): H2OFrame = asH2OFrame(rdd, None)
  def asH2OFrame(rdd: SupportedRDD, frameName: Option[String]): H2OFrame = H2OContext.toH2OFrame(sparkContext, rdd, frameName)
  def asH2OFrame(rdd: SupportedRDD, frameName: String): H2OFrame = asH2OFrame(rdd, Option(frameName))

  /** Transforms RDD[Supported type] to H2OFrame key */
  def toH2OFrameKey(rdd: SupportedRDD): Key[_] = toH2OFrameKey(rdd, None)
  def toH2OFrameKey(rdd: SupportedRDD, frameName: Option[String]): Key[_] = asH2OFrame(rdd, frameName)._key
  def toH2OFrameKey(rdd: SupportedRDD, frameName: String): Key[_] = toH2OFrameKey(rdd, Option(frameName))

  /** Transform DataFrame to H2OFrame */
  def asH2OFrame(df : DataFrame): H2OFrame = asH2OFrame(df, None)
  def asH2OFrame(df : DataFrame, frameName: Option[String]) : H2OFrame = H2OContext.toH2OFrame(sparkContext, df, if (frameName != null) frameName else None)
  def asH2OFrame(df : DataFrame, frameName: String) : H2OFrame = asH2OFrame(df, Option(frameName))

  /** Transform DataFrame to H2OFrame key */
  def toH2OFrameKey(df : DataFrame): Key[Frame] = toH2OFrameKey(df, None)
  def toH2OFrameKey(df : DataFrame, frameName: Option[String]) : Key[Frame] = asH2OFrame(df, frameName)._key
  def toH2OFrameKey(df : DataFrame, frameName: String) : Key[Frame] = toH2OFrameKey(df, Option(frameName))

  /** Create a new H2OFrame based on existing Frame referenced by its key.*/
  def asH2OFrame(s: String): H2OFrame = new H2OFrame(s)

  /** Create a new H2OFrame based on existing Frame */
  def asH2OFrame(fr: Frame): H2OFrame = new H2OFrame(fr)
  /**
   * Support for calls from Py4J
   */

  /** Conversion from RDD[String] to H2O's DataFrame */
  def asH2OFrameFromRDDString(rdd: JavaRDD[String], frameName: String): H2OFrame = H2OContext.toH2OFrameFromRDDString(sparkContext,rdd.rdd, Option(frameName))

  /** Returns key of the H2O's DataFrame conversed from RDD[String]*/
  def asH2OFrameFromRDDStringKey(rdd: JavaRDD[String], frameName: String): Key[Frame] = asH2OFrameFromRDDString(rdd, frameName)._key

  /** Conversion from RDD[Boolean] to H2O's DataFrame */
  def asH2OFrameFromRDDBool(rdd: JavaRDD[Boolean], frameName: String): H2OFrame = H2OContext.toH2OFrameFromRDDBool(sparkContext,rdd.rdd, Option(frameName))

  /** Returns key of the H2O's DataFrame conversed from RDD[Boolean]*/
  def asH2OFrameFromRDDBoolKey(rdd: JavaRDD[Boolean], frameName: String): Key[Frame] = asH2OFrameFromRDDBool(rdd, frameName)._key

  /** Conversion from RDD[Double] to H2O's DataFrame */
  def asH2OFrameFromRDDDouble(rdd: JavaRDD[Double], frameName: String): H2OFrame = H2OContext.toH2OFrameFromRDDDouble(sparkContext,rdd.rdd, Option(frameName))

  /** Returns key of the H2O's DataFrame conversed from RDD[Double]*/
  def asH2OFrameFromRDDDoubleKey(rdd: JavaRDD[Double], frameName: String): Key[Frame] = asH2OFrameFromRDDDouble(rdd, frameName)._key

  /** Conversion from RDD[Long] to H2O's DataFrame */
  def asH2OFrameFromRDDLong(rdd: JavaRDD[Long], frameName: String): H2OFrame = H2OContext.toH2OFrameFromRDDLong(sparkContext,rdd.rdd, Option(frameName))

  /** Returns key of the H2O's DataFrame conversed from RDD[Long]*/
  def asH2OFrameFromRDDLongKey(rdd: JavaRDD[Long], frameName: String): Key[Frame] = asH2OFrameFromRDDLong(rdd, frameName)._key

  /** Convert given H2O frame into a RDD type */
  @deprecated("Use asRDD instead", "0.2.3")
  def toRDD[A <: Product: TypeTag: ClassTag](fr : H2OFrame) : RDD[A] = asRDD[A](fr)

  /** Convert given H2O frame into a Product RDD type */
  def asRDD[A <: Product: TypeTag: ClassTag](fr : H2OFrame) : RDD[A] = createH2ORDD[A](fr)

  /** Convert given H2O frame into DataFrame type */
  @deprecated("1.3", "Use asDataFrame")
  def asSchemaRDD(fr : H2OFrame)(implicit sqlContext: SQLContext) : DataFrame = createH2OSchemaRDD(fr)
  def asDataFrame(fr : H2OFrame)(implicit sqlContext: SQLContext) : DataFrame = createH2OSchemaRDD(fr)
  def asDataFrame(s : String)(implicit sqlContext: SQLContext) : DataFrame = createH2OSchemaRDD(new H2OFrame(s))

  def h2oLocalClient = this.localClientIp + ":" + this.localClientPort

  def h2oLocalClientIp = this.localClientIp

  def h2oLocalClientPort = this.localClientPort

  // For now disable opening Spark UI
  //def sparkUI = sparkContext.ui.map(ui => ui.appUIAddress)

  /** Initialize Sparkling H2O and start H2O cloud with specified number of workers. */
  @deprecated(message = "Use start() method.", since = "1.4.11")
  def start(h2oWorkers: Int):H2OContext = {
    import H2OConf._
    sparkConf.set(PROP_CLUSTER_SIZE._1, h2oWorkers.toString)
    start()

  }

  /** Initialize Sparkling H2O and start H2O cloud. */
  def start(): H2OContext = {
    if(!isRunningOnCorrectSpark){
      throw new WrongSparkVersion(s"You are trying to use Sparkling Water built for Spark ${buildSparkMajorVersion}," +
        s" but your $$SPARK_HOME(=${sparkContext.getSparkHome().getOrElse("SPARK_HOME is not defined!")}) property" +
        s" points to Spark of version ${sparkContext.version}. Please ensure correct Spark is provided and" +
        s" re-run Sparkling Water.")
    }
    import H2OConf._
    // Setup properties for H2O configuration
    if (!sparkConf.contains(PROP_CLOUD_NAME._1)) {
      sparkConf.set(PROP_CLOUD_NAME._1,
                    PROP_CLOUD_NAME._2 + System.getProperty("user.name", "cluster") + "_" + Random.nextInt())
    }

    // Check Spark environment and reconfigure some values
    H2OContext.checkAndUpdateSparkEnv(sparkConf)
    logInfo(s"Starting H2O services: " + super[H2OConf].toString)
    // Create dummy RDD distributed over executors

    val (spreadRDD, spreadRDDNodes) = createSpreadRDD()

    if(isClusterTopologyListenerEnabled){
      //attach listener which shutdown H2O when we bump into executor we didn't discover during the spreadRDD phase
      sparkContext.addSparkListener(new SparkListener(){
        override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
          throw new IllegalArgumentException("Executor without H2O instance discovered, killing the cloud!")
        }
      })
    }
    // Start H2O nodes
    // Get executors to execute H2O
    val allExecutorIds = spreadRDDNodes.map(_.executorId).distinct
    val executorIds = allExecutorIds
    // The collected executors based on IDs should match
    assert(spreadRDDNodes.length == executorIds.length,
            s"Unexpected number of executors ${spreadRDDNodes.length}!=${executorIds.length}")
    // H2O is executed only on the subset of Spark cluster - fail
    if (executorIds.length < allExecutorIds.length) {
      throw new IllegalArgumentException(s"""Spark cluster contains ${allExecutorIds.length},
               but H2O is running only on ${executorIds.length} nodes!""")
    }
    // Execute H2O on given nodes
    logInfo(s"""Launching H2O on following ${spreadRDDNodes.length} nodes: ${spreadRDDNodes.mkString(",")}""")

    var h2oNodeArgs = getH2ONodeArgs
    // Disable web on h2o nodes in non-local mode
    if(!sparkContext.isLocal){
      h2oNodeArgs = h2oNodeArgs++Array("-disable_web")
    }
    logDebug(s"Arguments used for launching h2o nodes: ${h2oNodeArgs.mkString(" ")}")
    val executors = startH2O(sparkContext, spreadRDD, spreadRDDNodes.length, h2oNodeArgs)
    // Store runtime information
    h2oNodes.append( executors:_* )

    // Connect to a cluster via H2O client, but only in non-local case
    if (!sparkContext.isLocal) {
      logTrace("Sparkling H2O - DISTRIBUTED mode: Waiting for " + executors.length)
      // Get arguments for this launch including flatfile
      // And also ask for client mode
      val h2oClientIp = clientIp.getOrElse(getHostname(SparkEnv.get))
      val h2oClientArgs = toH2OArgs(getH2OClientArgs ++ Array("-ip", h2oClientIp, "-client"),
                              this,
                              executors)
      logDebug(s"Arguments used for launching h2o client node: ${h2oClientArgs.mkString(" ")}")
      // Launch H2O
      H2OStarter.start(h2oClientArgs, false)
    }
    // And wait for right cluster size
    H2O.waitForCloudSize(executors.length, cloudTimeout)
    // Register web API for client
    H2OContext.registerClientWebAPI(sparkContext, this)
    H2O.finalizeRegistration()
    // Fill information about H2O client
    localClientIp = H2O.SELF_ADDRESS.getHostAddress
    localClientPort = H2O.API_PORT

    // Inform user about status
    logInfo("Sparkling Water started, status of context: " + this.toString)
    this
  }

  /** Stops H2O context.
    *
    * Calls System.exit() which kills executor JVM.
    */
  def stop(stopSparkContext: Boolean = false): Unit = {
    if (stopSparkContext) sparkContext.stop()
    H2O.orderlyShutdown(1000)
    H2O.exit(0)
  }

  private def createSpreadRDD() = new SpreadRDDBuilder(sparkContext,
                                                       H2OContextUtils.guessTotalExecutorSize(sparkContext)).build()


  def createH2ORDD[A <: Product: TypeTag: ClassTag](fr: H2OFrame): RDD[A] = {
    new H2ORDD[A](this,fr)
  }

  def createH2OSchemaRDD(fr: H2OFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val h2oSchemaRDD = new H2OSchemaRDD(this, fr)
    sqlContext.createDataFrame(h2oSchemaRDD, H2OSchemaUtils.createSchema(fr))
  }

  /** Open H2O Flow running in this client. */
  def openFlow(): Unit = openURI(s"http://${h2oLocalClient}")
  /** Open Spark task manager. */
  //def openSparkUI(): Unit = sparkUI.foreach(openURI(_))

  /** Open browser for given address.
    *
    * @param uri addres to open in browser, e.g., http://example.com
    */
  private def openURI(uri: String): Unit = {
    import java.awt.Desktop
    if (!isTesting) {
      if (Desktop.isDesktopSupported) {
        Desktop.getDesktop.browse(new java.net.URI(uri))
      } else {
        logWarning(s"Desktop support is missing! Cannot open browser for ${uri}")
      }
    }
  }

  /**
   * Return true if running inside spark/sparkling water test.
   *
   * @return true if the actual run is test run
    * @return true if the actual run is test run
   */
  private def isTesting = sparkContext.conf.contains("spark.testing") || sys.props.contains("spark.testing")

  override def toString: String = {
    s"""
      |Sparkling Water Context:
      | * H2O name: ${H2O.ARGS.name}
      | * number of executors: ${h2oNodes.size}
      | * list of used executors:
      |  (executorId, host, port)
      |  ------------------------
      |  ${h2oNodes.mkString("\n  ")}
      |  ------------------------
      |
      |  Open H2O Flow in browser: http://${h2oLocalClient} (CMD + click in Mac OSX)
    """.stripMargin
  }

  /** Checks whether version of provided Spark is the same as Spark's version designated for this Sparkling Water version.
    * We check for correct version in shell scripts and during the build but we need to do the check also in the code in cases when the user
    * executes for example spark-shell command with sparkling water assembly jar passed as --jars and initiates H2OContext.
    * (Because in that case no check for correct Spark version has been done so far.)
    */
  private def isRunningOnCorrectSpark = sparkContext.version.startsWith(buildSparkMajorVersion)

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /** Define implicits available via h2oContext.implicits._*/
  object implicits extends H2OContextImplicits with Serializable {
    protected override def _h2oContext: H2OContext = self
  }
  // scalastyle:on
  H2OContext.setInstantiatedContext(this)
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

  @transient private val instantiatedContext = new AtomicReference[H2OContext]()

  private def getOrCreate(sc: SparkContext, h2oWorkers: Option[Int]): H2OContext = synchronized {
    if (instantiatedContext.get() == null) {
      instantiatedContext.set(new H2OContext(sc))
      if(h2oWorkers.isEmpty){
        instantiatedContext.get().start()
      }else{
        instantiatedContext.get().start(h2oWorkers.get)
      }
    }
    instantiatedContext.get()
  }

  /**
    * Get existing H2O Context or initialize Sparkling H2O and start H2O cloud with specified number of workers
    *
    * @param sc Spark Context
    * @return H2O Context
    */
  @deprecated(message = "Use getOrCreate(sc: SparkContext) method.", since = "1.5.11")
  def getOrCreate(sc: SparkContext, h2oWorkers: Int): H2OContext = {
    getOrCreate(sc, Some(h2oWorkers))
  }

  /**
    * Get existing H2O Context or initialize Sparkling H2O and start H2O cloud with default number of workers
    *
    * @param sc Spark Context
    * @return H2O Context
    */
  def getOrCreate(sc: SparkContext): H2OContext = {
    getOrCreate(sc, None)
  }

  /** Supports call from java environments. */
  def getOrCreate(sc: JavaSparkContext): H2OContext = {
    getOrCreate(sc.sc, None)
  }

  /** Supports call from java environments. */
  @deprecated(message = "Use getOrCreate(sc: JavaSparkContext) method.", since = "1.5.11")
  def getOrCreate(sc: JavaSparkContext, h2oWorkers: Int): H2OContext = {
    getOrCreate(sc.sc,Some(h2oWorkers))
  }

  /** Transform SchemaRDD into H2O Frame */
  def toH2OFrame(sc: SparkContext, dataFrame: DataFrame, frameKeyName: Option[String]) : H2OFrame = {
    import org.apache.spark.h2o.H2OSchemaUtils._
    // Cache DataFrame RDD's
    val dfRdd = dataFrame.rdd

    val keyName = frameKeyName.getOrElse("frame_rdd_" + dfRdd.id)
    // Fetch cached frame from DKV
    val frameVal = DKV.get(keyName)
    if (frameVal==null) {
      // Flattens and expands RDD's schema
      val flatRddSchema = expandedSchema(sc, dataFrame)
      // Patch the flat schema based on information about types
      val fnames = flatRddSchema.map(t => t._2.name).toArray
      // Transform datatype into h2o types
      val vecTypes = flatRddSchema.indices
        .map(idx => {
          val f = flatRddSchema(idx)
          dataTypeToVecType(f._2.dataType)
        }).toArray
      // Prepare frame header and put it into DKV under given name
      initFrame(keyName, fnames)
      // Create a new chunks corresponding to spark partitions
      // Note: Eager, not lazy, evaluation
      val rows = sc.runJob(dfRdd, perSQLPartition(keyName, flatRddSchema, vecTypes) _)
      val res = new Array[Long](dfRdd.partitions.size)
      rows.foreach { case (cidx, nrows) => res(cidx) = nrows}

      // Add Vec headers per-Chunk, and finalize the H2O Frame
      new H2OFrame(
        finalizeFrame(
          keyName,
          res,
          vecTypes))
    } else {
      new H2OFrame(frameVal.get.asInstanceOf[Frame])
    }
  }

  private def inferFieldType(value : Any): Class[_] ={
    value match {
      case n: Byte  => classOf[java.lang.Byte]
      case n: Short => classOf[java.lang.Short]
      case n: Int => classOf[java.lang.Integer]
      case n: Long => classOf[java.lang.Long]
      case n: Float => classOf[java.lang.Float]
      case n: Double => classOf[java.lang.Double]
      case n: Boolean => classOf[java.lang.Boolean]
      case n: String => classOf[java.lang.String]
      case n: java.sql.Timestamp => classOf[java.sql.Timestamp]
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }

  def toH2OFrameFromPureProduct(sc: SparkContext, rdd: RDD[Product], frameKeyName: Option[String]): H2OFrame = {
    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand()) // There are uniq IDs for RDD

    // infer the type
    val first = rdd.first()
    val fnames = 0.until(first.productArity).map(idx => "f" + idx).toArray[String]
    val ftypes = new ListBuffer[Class[_]]()
    val it = first.productIterator
    while(it.hasNext){
      ftypes+=inferFieldType(it.next())
    }
    // Collect H2O vector types for all input types
    val vecTypes = ftypes.toArray[Class[_]].indices.map(idx => dataTypeToVecType(ftypes(idx))).toArray
    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, fnames)
    // Create chunks on remote nodes
    val rows = sc.runJob(rdd, perTypedRDDPartition(keyName, vecTypes) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.length)
    rows.foreach{ case(cidx, nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new H2OFrame(finalizeFrame(keyName, res, vecTypes))
  }
  /** Transform typed RDD into H2O Frame */
  def toH2OFrame[A <: Product : TypeTag](sc: SparkContext, rdd: RDD[A], frameKeyName: Option[String]) : H2OFrame = {
    import org.apache.spark.h2o.H2OTypeUtils._
    import org.apache.spark.h2o.ReflectionUtils._

    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand()) // There are uniq IDs for RDD
    val fnames = names[A]
    val ftypes = types[A](fnames)
    // Collect H2O vector types for all input types
    val vecTypes = ftypes.indices.map(idx => dataTypeToVecType(ftypes(idx))).toArray
    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, fnames)
    // Create chunks on remote nodes
    val rows = sc.runJob(rdd, perTypedRDDPartition(keyName, vecTypes) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.length)
    rows.foreach{ case(cidx, nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new H2OFrame(finalizeFrame(keyName, res, vecTypes))
  }

  /** Transform supported type for conversion to H2OFrame*/
  def toH2OFrame(sc: SparkContext, rdd: SupportedRDD, frameKeyName: Option[String]): H2OFrame = rdd.toH2OFrame(sc, frameKeyName)

  /** Transform RDD[String] to appropriate H2OFrame */
  def toH2OFrameFromRDDString(sc: SparkContext, rdd: RDD[String], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitiveRDD(sc, rdd, frameKeyName)

  /** Transform RDD[Int] to appropriate H2OFrame */
  def toH2OFrameFromRDDInt(sc: SparkContext, rdd: RDD[Int], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitiveRDD(sc, rdd, frameKeyName)

  /** Transform RDD[Float] to appropriate H2OFrame */
  def toH2OFrameFromRDDFloat(sc: SparkContext, rdd: RDD[Float], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitiveRDD(sc, rdd, frameKeyName)

  /** Transform RDD[Double] to appropriate H2OFrame */
  def toH2OFrameFromRDDDouble(sc: SparkContext, rdd: RDD[Double], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitiveRDD(sc, rdd, frameKeyName)

  /** Transform RDD[Long] to appropriate H2OFrame */
  def toH2OFrameFromRDDLong(sc: SparkContext, rdd: RDD[Long], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitiveRDD(sc, rdd, frameKeyName)

  /** Transform RDD[Double] to appropriate H2OFrame */
  def toH2OFrameFromRDDBool(sc: SparkContext, rdd: RDD[Boolean], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitiveRDD(sc, rdd, frameKeyName)

  /** Transform RDD[Short] to appropriate H2OFrame */
  def toH2OFrameFromRDDShort(sc: SparkContext, rdd: RDD[Short], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitiveRDD(sc, rdd, frameKeyName)

  /** Transform RDD[Byte] to appropriate H2OFrame */
  def toH2OFrameFromRDDByte(sc: SparkContext, rdd: RDD[Byte], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitiveRDD(sc, rdd, frameKeyName)

  /** Transform RDD[Byte] to appropriate H2OFrame */
  def toH2OFrameFromRDDTimeStamp(sc: SparkContext, rdd: RDD[java.sql.Timestamp], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitiveRDD(sc, rdd, frameKeyName) // scalastyle:ignore

  private[this]
  def toH2OFrameFromPrimitiveRDD[T: TypeTag](sc: SparkContext, rdd: RDD[T], frameKeyName: Option[String]): H2OFrame = {
    import org.apache.spark.h2o.H2OTypeUtils._
    import org.apache.spark.h2o.ReflectionUtils._

    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand())

    val fnames = Array[String]("values")
    val ftypes = Array[Class[_]](typ(typeOf[T]))
    val vecTypes = ftypes.indices.map(idx => dataTypeToVecType(ftypes(idx))).toArray

    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, fnames)

    val rows = sc.runJob(rdd, perPrimitiveRDDPartition(keyName, vecTypes) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.length)
    rows.foreach { case (cidx, nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new H2OFrame(finalizeFrame(keyName, res, vecTypes))
  }


  /** Transform RDD[LabeledPoint] to appropriate H2OFrame */
  def toH2OFrameFromRDDLabeledPoint(sc: SparkContext, rdd: RDD[LabeledPoint], frameKeyName: Option[String]): H2OFrame = {
    import org.apache.spark.h2o.H2OTypeUtils._
    import org.apache.spark.h2o.ReflectionUtils._
    // first convert vector to dense vector
    val rddDense = rdd.map(labeledPoint => new LabeledPoint(labeledPoint.label,labeledPoint.features.toDense))
    val numFeatures = rddDense.map(labeledPoint => labeledPoint.features.size)
    val maxNumFeatures = numFeatures.max()
    val minNumFeatures = numFeatures.min()
    if(minNumFeatures<maxNumFeatures){
      // Features vectors of different sizes, filling missing with n/a
      logWarning("WARNING: Converting RDD[LabeledPoint] to H2OFrame where features vectors have different size, filling missing with n/a")
    }
    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand())
    val fnames = (Seq[String]("label") ++ 0.until(maxNumFeatures).map(num => "feature" + num).toSeq).toArray[String]
    val ftypes = 0.until(maxNumFeatures + 1).map(_ => typ(typeOf[Double]))
    val vecTypes = ftypes.indices.map(idx => dataTypeToVecType(ftypes(idx))).toArray
    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, fnames)
    val rows = sc.runJob(rddDense, perLabeledPointRDDPartition(keyName, vecTypes, maxNumFeatures) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.length)
    rows.foreach { case (cidx,  nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new H2OFrame(finalizeFrame(keyName, res, vecTypes))
  }

  private
  def perPrimitiveRDDPartition[T](keystr: String, vecTypes: Array[Byte])
                              (context: TaskContext, it: Iterator[T]): (Int, Long) = {
    // An array of H2O NewChunks; A place to record all the data in this partition
    val nchks = water.fvec.FrameUtils.createNewChunks(keystr, vecTypes, context.partitionId)
    // Helper to hold H2O string
    val valStr = new BufferedString()
    it.foreach(r => {
      // For all rows in RDD
      val chk = nchks(0) // There is only one chunk
      r match {
        case n: Number  => chk.addNum(n.doubleValue())
        case n: Boolean => chk.addNum(if (n) 1 else 0)
        case n: String  => chk.addStr(valStr.setTo(n))
        case n : java.sql.Timestamp => chk.addNum(n.asInstanceOf[java.sql.Timestamp].getTime())
        case _ => chk.addNA()
      }
    })
    // Compress & write out the Partition/Chunks
    water.fvec.FrameUtils.closeNewChunks(nchks)
    // Return Partition# and rows in this Partition
    (context.partitionId, nchks(0)._len)
  }
  private
  def perSQLPartition ( keystr: String, types: Seq[(Seq[Int], StructField, Byte)], vecTypes: Array[Byte])
                      ( context: TaskContext, it: Iterator[Row] ): (Int,Long) = {
    // New chunks on remote node
    val nchks = water.fvec.FrameUtils.createNewChunks(keystr, vecTypes, context.partitionId)
    val valStr = new BufferedString() // just helper for string columns
    it.foreach(row => {
      var startOfSeq = -1
      // Fill row in the output frame
      types.indices.foreach { idx => // Index of column
        val chk = nchks(idx)
        val field = types(idx)
        val path = field._1
        val dataType = field._2.dataType
        // Helpers to distinguish embedded collection types
        val isAry = field._3 == H2OSchemaUtils.ARRAY_TYPE
        val isVec = field._3 == H2OSchemaUtils.VEC_TYPE
        val isNewPath = if (idx > 0) path != types(idx-1)._1 else true
        // Reset counter for sequences
        if ((isAry || isVec) && isNewPath) startOfSeq = idx
        else if (!isAry && !isVec) startOfSeq = -1

        var i = 0
        var subRow = row
        while (i < path.length-1 && !subRow.isNullAt(path(i))) {
          subRow = subRow.getAs[Row](path(i)); i += 1
        }
        val aidx = path(i) // actual index into row provided by path
        if (subRow.isNullAt(aidx)) {
          chk.addNA()
        } else {
          val ary = if (isAry) subRow.getAs[Seq[_]](aidx) else null
          val aryLen = if (isAry) ary.length else -1
          val aryIdx = idx - startOfSeq // shared index to position in array/vector
          val vec = if (isVec) subRow.getAs[mllib.linalg.Vector](aidx) else null
          if (isAry && aryIdx >= aryLen) chk.addNA()
          else if (isVec && aryIdx >= vec.size) chk.addNum(0.0) // Add zeros for vectors
          else dataType match {
            case BooleanType => chk.addNum(if (isAry)
              if (ary(aryIdx).asInstanceOf[Boolean]) 1 else 0
              else if (subRow.getBoolean(aidx)) 1 else 0)
            case BinaryType =>
            case ByteType => chk.addNum(if (isAry) ary(aryIdx).asInstanceOf[Byte] else subRow.getByte(aidx))
            case ShortType => chk.addNum(if (isAry) ary(aryIdx).asInstanceOf[Short] else subRow.getShort(aidx))
            case IntegerType => chk.addNum(if (isAry) ary(aryIdx).asInstanceOf[Int] else subRow.getInt(aidx))
            case LongType => chk.addNum(if (isAry) ary(aryIdx).asInstanceOf[Long] else subRow.getLong(aidx))
            case FloatType => chk.addNum(if (isAry) ary(aryIdx).asInstanceOf[Float] else subRow.getFloat(aidx))
            case DoubleType => chk.addNum(if (isAry) {
                ary(aryIdx).asInstanceOf[Double]
              } else {
                if (isVec) {
                  subRow.getAs[mllib.linalg.Vector](aidx)(idx - startOfSeq)
                } else {
                  subRow.getDouble(aidx)
                }
              })
            case StringType => {
              val sv = if (isAry) ary(aryIdx).asInstanceOf[String] else subRow.getString(aidx)
              // Always produce string vectors
              chk.addStr(valStr.setTo(sv))
            }
            case TimestampType => chk.addNum(row.getAs[java.sql.Timestamp](aidx).getTime())
            case _ => chk.addNA()
          }
        }

      }
    })
    // Compress & write out the Partition/Chunks
    water.fvec.FrameUtils.closeNewChunks(nchks)
    // Return Partition# and rows in this Partition
    (context.partitionId,nchks(0)._len)
  }

  private
  def perTypedRDDPartition[A<:Product](keystr:String, vecTypes: Array[Byte])
                                 ( context: TaskContext, it: Iterator[A] ): (Int,Long) = {
    // An array of H2O NewChunks; A place to record all the data in this partition
    val nchks = water.fvec.FrameUtils.createNewChunks(keystr, vecTypes, context.partitionId)

    val valStr = new BufferedString()
    it.foreach(prod => { // For all rows which are subtype of Product
      for( i <- 0 until prod.productArity ) { // For all fields...
        val fld = prod.productElement(i)
        val chk = nchks(i)
        val x = fld match {
            case Some(n) => n
            case _ => fld
        }
        x match {
          case n: Number  => chk.addNum(n.doubleValue())
          case n: Boolean => chk.addNum(if (n) 1 else 0)
          case n: String  => chk.addStr(valStr.setTo(n))
          case n : java.sql.Timestamp => chk.addNum(n.asInstanceOf[java.sql.Timestamp].getTime())
          case _ => chk.addNA()
        }
      }
    })
    // Compress & write out the Partition/Chunks
    water.fvec.FrameUtils.closeNewChunks(nchks)
    // Return Partition# and rows in this Partition
    (context.partitionId,nchks(0)._len)
  }

  private
  def perLabeledPointRDDPartition(keystr: String, vecTypes: Array[Byte], maxNumFeatures: Int)
                                 (context: TaskContext, it: Iterator[LabeledPoint]): (Int, Long) = {
    // An array of H2O NewChunks; A place to record all the data in this partition
    val chunks = water.fvec.FrameUtils.createNewChunks(keystr, vecTypes, context.partitionId)
    it.foreach(labeledPoint => {
      // For all LabeledPoints in RDD
      var nextChunkId = 0

      // Add LabeledPoint label
      chunks(nextChunkId).addNum(labeledPoint.label)
      nextChunkId = nextChunkId + 1

      for( i<-0 until labeledPoint.features.size) {
        // For all features...
        chunks(nextChunkId).addNum(labeledPoint.features(i))
        nextChunkId =nextChunkId + 1
      }

      for( i<-labeledPoint.features.size until maxNumFeatures){
        // Fill missing features with n/a
        chunks(nextChunkId).addNA()
        nextChunkId = nextChunkId + 1
      }
    })
    // Compress & write out the Partition/Chunks
    water.fvec.FrameUtils.closeNewChunks(chunks)
    // Return Partition# and rows in this Partition
    (context.partitionId, chunks(0)._len)
  }

  private
  def initFrame[T](keyName: String, names: Array[String]):Unit = {
    val fr = new water.fvec.Frame(Key.make(keyName))
    water.fvec.FrameUtils.preparePartialFrame(fr, names)
    // Save it directly to DKV
    fr.update()
  }

  private
  def finalizeFrame[T](keyName: String,
                       res: Array[Long],
                       colTypes: Array[Byte],
                       colDomains: Array[Array[String]] = null):Frame = {
    val fr:Frame = DKV.get(keyName).get.asInstanceOf[Frame]
    water.fvec.FrameUtils.finalizePartialFrame(fr, res, colDomains, colTypes)
    fr
  }

  /** Check Spark environment and warn about possible problems. */
  private
  def checkAndUpdateSparkEnv(conf: SparkConf): Unit = {
    // If 'spark.executor.instances' is specified update H2O property as well
    conf.getOption("spark.executor.instances").foreach(v => conf.set("spark.ext.h2o.cluster.size", v))
    // Increase locality timeout since h2o-specific tasks can be long computing
    if (conf.getInt("spark.locality.wait", 3000) <= 3000) {
      logWarning(s"Increasing 'spark.locality.wait' to value 30000")
      conf.set("spark.locality.wait", "30000")
    }

    if (!conf.contains("spark.scheduler.minRegisteredResourcesRatio")) {
      logWarning("The property 'spark.scheduler.minRegisteredResourcesRatio' is not specified!\n" +
                 "We recommend to pass `--conf spark.scheduler.minRegisteredResourcesRatio=1`")
      // Setup the property but at this point it does not make good sense
      conf.set("spark.scheduler.minRegisteredResourcesRatio", "1")
    }
  }
  private[h2o] def registerClientWebAPI(sc: SparkContext, h2oContext: H2OContext): Unit = {
    if(h2oContext.isH2OReplEnabled){
      registerScalaIntEndp(sc)
    }
    registerDataFramesEndp(sc, h2oContext)
    registerH2OFramesEndp(sc, h2oContext)
    registerRDDsEndp(sc, h2oContext)
  }

  private def registerH2OFramesEndp(sc: SparkContext, h2oContext: H2OContext) = {

    val h2oFramesHandler = new H2OFramesHandler(sc, h2oContext)

    def h2oFramesFactory = new HandlerFactory {
      override def create(handler: Class[_ <: Handler]): Handler = h2oFramesHandler
    }

    RequestServer.register("/3/h2oframes/(?<h2oframe_id>.*)/dataframe", "POST",
                           classOf[H2OFramesHandler], "toDataFrame",
                           null,
                           "Transform H2OFrame with given ID to Spark's DataFrame",
                           h2oFramesFactory)

  }

  private def registerRDDsEndp(sc: SparkContext, h2oContext: H2OContext) = {

    val rddsHandler = new RDDsHandler(sc, h2oContext)

    def rddsFactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = rddsHandler
    }

    RequestServer.register("/3/RDDs", "GET",
                           classOf[RDDsHandler], "list",
                           null,
                           "Return all RDDs within Spark cloud",
                           rddsFactory)

    RequestServer.register("/3/RDDs/(?<rdd_id>[0-9]+)", "POST",
                           classOf[RDDsHandler], "getRDD",
                           null,
                           "Get RDD with the given ID from Spark cloud",
                           rddsFactory)

    RequestServer.register("/3/RDDs/(?<rdd_id>[0-9a-zA-Z_]+)/h2oframe", "POST",
      classOf[RDDsHandler], "toH2OFrame",
      null,
      "Transform RDD with the given ID to H2OFrame",
      rddsFactory)

  }

  private def registerDataFramesEndp(sc: SparkContext, h2oContext: H2OContext) = {

    val dataFramesHandler = new DataFramesHandler(sc, h2oContext)

    def dataFramesfactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = dataFramesHandler
    }

    RequestServer.register("/3/dataframes", "GET",
                           classOf[DataFramesHandler], "list",
                           null,
                           "Return all Spark's DataFrames",
                           dataFramesfactory)

    RequestServer.register("/3/dataframes/(?<dataframe_id>[0-9a-zA-Z_]+)", "POST",
                           classOf[DataFramesHandler], "getDataFrame",
                           null,
                           "Get Spark's DataFrame with the given ID",
                           dataFramesfactory)

    RequestServer.register("/3/dataframes/(?<dataframe_id>[0-9a-zA-Z_]+)/h2oframe", "POST",
                           classOf[DataFramesHandler], "toH2OFrame",
                           null,
                           "Transform Spark's DataFrame with the given ID to H2OFrame",
                           dataFramesfactory)

  }

  private def registerScalaIntEndp(sc: SparkContext) = {
    val scalaCodeHandler = new ScalaCodeHandler(sc)
    def scalaCodeFactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = scalaCodeHandler
    }
    RequestServer.register("/3/scalaint/(?<session_id>[0-9]+)", "POST",
                           classOf[ScalaCodeHandler], "interpret",
                           null,
                           "Interpret the code and return the result",
                           scalaCodeFactory)

    RequestServer.register("/3/scalaint", "POST",
                           classOf[ScalaCodeHandler], "initSession",
                           null,
                           "Return session id for communication with scala interpreter",
                           scalaCodeFactory)

    RequestServer.register("/3/scalaint", "GET",
                           classOf[ScalaCodeHandler], "getSessions",
                           null,
                           "Return all active session IDs",
                           scalaCodeFactory)

    RequestServer.register("/3/scalaint/(?<session_id>[0-9]+)", "DELETE",
                           classOf[ScalaCodeHandler], "destroySession",
                           null,
                           "Return session id for communication with scala interpreter",
                           scalaCodeFactory)
  }
}

class WrongSparkVersion(msg: String) extends Exception(msg) with NoStackTrace {}