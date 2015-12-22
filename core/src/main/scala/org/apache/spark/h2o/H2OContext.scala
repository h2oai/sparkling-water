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

import java.net.{URLClassLoader, URL}

import org.apache.spark._
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.h2o.H2OContextUtils._
import org.apache.spark.rdd.{H2ORDD, H2OSchemaRDD}
import org.apache.spark.scheduler.{SparkListenerExecutorAdded, SparkListener}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import water._
import water.api.DataFrames.DataFramesHandler
import water.api.H2OFrames.H2OFramesHandler
import water.api.RDDs.RDDsHandler
import water.api._
import water.api.scalaInt.ScalaCodeHandler
import water.parser.BufferedString

import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Random

/**
 * Simple H2O context motivated by SQLContext.
 *
 * It provides implicit conversion from RDD -> H2OLikeRDD and back.
 */
class H2OContext private[this](@transient val sparkContext: SparkContext) extends {
    val sparkConf = sparkContext.getConf
  } with org.apache.spark.Logging
  with H2OConf
  with Serializable {

  /** Runtime list of active H2O nodes */
  private val h2oNodes = mutable.ArrayBuffer.empty[NodeDesc]

  /** IP of H2O client */
  private var localClientIp: String = _
  /** REST port of H2O client */
  private var localClientPort: Int = _

  /** Implicit conversion from Spark DataFrame to H2O's DataFrame */
  implicit def asH2OFrame(df : DataFrame) : H2OFrame = asH2OFrame(df, None)
  def asH2OFrame(df : DataFrame, frameName: Option[String]) : H2OFrame = H2OContext.toH2OFrame(sparkContext, df, if (frameName != null) frameName else None)
  def asH2OFrame(df : DataFrame, frameName: String) : H2OFrame = asH2OFrame(df, Option(frameName))

  /** Implicit conversion from typed RDD to H2O's DataFrame */
  implicit def asH2OFrame[A <: Product : TypeTag](rdd : RDD[A]) : H2OFrame = asH2OFrame(rdd, None)
  def asH2OFrame[A <: Product : TypeTag](rdd : RDD[A], frameName: Option[String]) : H2OFrame = H2OContext.toH2OFrame(sparkContext, rdd, frameName)
  def asH2OFrame[A <: Product : TypeTag](rdd : RDD[A], frameName: String) : H2OFrame = asH2OFrame(rdd, Option(frameName))

  /** Implicit conversion from RDD[Primitive type] ( where primitive type can be String, Double, Float or Int) to appropriate H2OFrame */
  implicit def asH2OFrame(primitiveType: PrimitiveType): H2OFrame = asH2OFrame(primitiveType, None)
  def asH2OFrame(primitiveType: PrimitiveType, frameName: Option[String]): H2OFrame = H2OContext.toH2OFrame(sparkContext, primitiveType, frameName)
  def asH2OFrame(primitiveType: PrimitiveType, frameName: String): H2OFrame = asH2OFrame(primitiveType, Option(frameName))

  /** Implicit conversion from Spark DataFrame to H2O's DataFrame */
  implicit def toH2OFrameKey(rdd : DataFrame) : Key[Frame] = toH2OFrameKey(rdd, None)
  def toH2OFrameKey(rdd : DataFrame, frameName: Option[String]) : Key[Frame] = asH2OFrame(rdd, frameName)._key
  def toH2OFrameKey(rdd : DataFrame, frameName: String) : Key[Frame] = toH2OFrameKey(rdd, Option(frameName))

  /** Implicit conversion from typed RDD to H2O's DataFrame */
  implicit def toH2OFrameKey[A <: Product : TypeTag](rdd : RDD[A]) : Key[_] = toH2OFrameKey(rdd, None)
  def toH2OFrameKey[A <: Product : TypeTag](rdd : RDD[A], frameName: Option[String]) : Key[_] = asH2OFrame(rdd, frameName)._key
  def toH2OFrameKey[A <: Product : TypeTag](rdd : RDD[A], frameName: String) : Key[_] = toH2OFrameKey(rdd, Option(frameName))

  /** Implicit conversion from RDD[Primitive type] ( where primitive type can be String, Boolean, Double, Float, Int,
    * Long, Short or Byte ) to appropriate H2O's DataFrame */
  implicit def toH2OFrameKey(primitiveType: PrimitiveType): Key[_] = toH2OFrameKey(primitiveType, None)
  def toH2OFrameKey(primitiveType: PrimitiveType, frameName: Option[String]): Key[_] = asH2OFrame(primitiveType, frameName)._key
  def toH2OFrameKey(primitiveType: PrimitiveType, frameName: String): Key[_] = toH2OFrameKey(primitiveType, Option(frameName))


  /** Implicit conversion from Frame to DataFrame */
  implicit def asH2OFrame(fr: Frame) : H2OFrame = new H2OFrame(fr)

  def asH2OFrame(s: String): H2OFrame = new H2OFrame(s)

  /** Returns a key of given frame */
  implicit def toH2OFrameKey(fr: Frame): Key[Frame] = fr._key

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

  /** Transform given Scala symbol to String */
  implicit def symbolToString(sy: scala.Symbol): String = sy.name

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


  /** Detected number of Spark executors
    * Property value is derived from SparkContext during creation of H2OContext. */
  private def numOfSparkExecutors = if (sparkContext.isLocal) 1 else sparkContext.getExecutorStorageStatus.length - 1

  def h2oLocalClient = this.localClientIp + ":" + this.localClientPort

  def h2oLocalClientIp = this.localClientIp

  def h2oLocalClientPort = this.localClientPort

  // For now disable opening Spark UI
  //def sparkUI = sparkContext.ui.map(ui => ui.appUIAddress)

  /** Initialize Sparkling H2O and start H2O cloud with specified number of workers. */
  private def start(h2oWorkers: Int):H2OContext = {
    import H2OConf._
    sparkConf.set(PROP_CLUSTER_SIZE._1, h2oWorkers.toString)
    start()

  }

  /**
    *Specifies maximum number of iterations where the number of executors remained the same
    * The spreadRDD function is stopped once the variable numTriesSame reached this number
    */
  private final val SUBSEQUENT_NUM_OF_TRIES=3

  /** Initialize Sparkling H2O and start H2O cloud. */
  private def start(): H2OContext = {
    import H2OConf._
    // Setup properties for H2O configuration
    if (!sparkConf.contains(PROP_CLOUD_NAME._1)) {
      sparkConf.set(PROP_CLOUD_NAME._1,
                    PROP_CLOUD_NAME._2 + System.getProperty("user.name", "cloud_" + Random.nextInt(42)))
    }

    // Check Spark environment and reconfigure some values
    H2OContext.checkAndUpdateSparkEnv(sparkConf)
    logInfo(s"Starting H2O services: " + super[H2OConf].toString)
    // Create dummy RDD distributed over executors

    val (spreadRDD, spreadRDDNodes) = createSpreadRDD(numRddRetries, drddMulFactor, numH2OWorkers, 0)

    //attach listener which shutdown H2O when we bump into executor we didn't discover during the spreadRDD phase
    sparkContext.addSparkListener(new SparkListener(){
      override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        throw new IllegalArgumentException("Executor without H2O instance discovered, killing the cloud!")
      }
    })
    // Start H2O nodes
    // Get executors to execute H2O
    val allExecutorIds = spreadRDDNodes.map(_._1).distinct
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
    logInfo(s"""Launching H2O on following nodes: ${spreadRDDNodes.mkString(",")}""")

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
      val h2oClientIp = clientIp.getOrElse(getIp(SparkEnv.get))
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

  @tailrec
  private
  def createSpreadRDD(nretries: Int,
                      mfactor: Int,
                      nworkers: Int, numTriesSame: Int): (RDD[NodeDesc], Array[NodeDesc]) = {
    logDebug(s"  Creating RDD for launching H2O nodes (mretries=${nretries}, mfactor=${mfactor}, nworkers=${nworkers}")
    // Non-positive value of nworkers means automatic detection of number of workers
    val nSparkExecBefore = numOfSparkExecutors
    val workers = if (nworkers > 0) nworkers else if (nSparkExecBefore > 0) nSparkExecBefore else defaultCloudSize
    val spreadRDD =
      sparkContext.parallelize(0 until mfactor*workers,
        mfactor*workers).persist()

    // Collect information about executors in Spark cluster
    val nodes = collectNodesInfo(spreadRDD)
    // Verify that all executors participate in execution
    val nSparkExecAfter = numOfSparkExecutors
    val sparkExecutors = nodes.map(_._1).distinct.length
    // Delete RDD
    spreadRDD.unpersist()
    if ((sparkExecutors < nworkers || nSparkExecAfter != nSparkExecBefore)
          && nretries == 0) {
      throw new IllegalArgumentException(
        s"""Cannot execute H2O on all Spark executors:
            | Expected number of H2O workers is ${nworkers}
            | Detected number of Spark workers is $sparkExecutors
            | Num of Spark executors before is $nSparkExecBefore
            | Num of Spark executors after is $nSparkExecAfter
            |
            | If you are running regular application, please, specify number of Spark workers
            | via ${H2OConf.PROP_CLUSTER_SIZE._1} Spark configuration property.
            | If you are running from shell,
            | you can try: val h2oContext = H2OContext.getOrCreate(sc,<number of Spark workers>)
            |
            |""".stripMargin
      )
    } else if (nSparkExecAfter != nSparkExecBefore) {
      // Repeat if we detect change in number of executors reported by storage level
      logInfo(s"Detected ${nSparkExecBefore} before, and ${nSparkExecAfter} spark executors after! Retrying again...")
      createSpreadRDD(nretries-1, mfactor, nworkers, 0)
    } else if ((nworkers>0 && sparkExecutors == nworkers || nworkers<=0) && sparkExecutors == nSparkExecAfter || numTriesSame==SUBSEQUENT_NUM_OF_TRIES) {
      // Return result only if we are sure that number of detected executors seems ok or if the number of executors didn't change in the last
      // SUBSEQUENT_NUM_OF_TRIES iterations
      logInfo(s"Detected ${sparkExecutors} spark executors for ${nworkers} H2O workers!")
      (new InvokeOnNodesRDD(nodes, sparkContext), nodes)
    } else {
      logInfo(s"Detected ${sparkExecutors} spark executors for ${nworkers} H2O workers! Retrying again...")

      createSpreadRDD(nretries-1, mfactor*2, nworkers, numTriesSame + 1)
    }
  }

  def createH2ORDD[A <: Product: TypeTag: ClassTag](fr: H2OFrame): RDD[A] = {
    new H2ORDD[A](this,fr)
  }

  def createH2OSchemaRDD(fr: H2OFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val h2oSchemaRDD = new H2OSchemaRDD(this, fr)
    import org.apache.spark.sql.H2OSQLContextUtils.internalCreateDataFrame
    internalCreateDataFrame(h2oSchemaRDD, H2OSchemaUtils.createSchema(fr))(sqlContext)
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
}

object H2OContext extends Logging {

  private[this] var instance: H2OContext = null

  private def getOrCreate(sc: SparkContext, h2oWorkers: Option[Int]): H2OContext = {
    if (instance == null) {
      val h2oContextClazz = classOf[H2OContext]
      val ctor = h2oContextClazz.getDeclaredConstructor(classOf[SparkContext])
      ctor.setAccessible(true)
      instance = ctor.newInstance(sc)
      if (h2oWorkers.isEmpty) {
        instance.start()
      } else {
        instance.start(h2oWorkers.get)
      }
    }
    instance
  }

  /**
    * Get existing H2O Context or initialize Sparkling H2O and start H2O cloud with specified number of workers
    *
    * @param sc Spark Context
    * @return H2O Context
    */
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

  /** Transform typed RDD into H2O Frame */
  def toH2OFrame[A <: Product : TypeTag](sc: SparkContext, rdd: RDD[A], frameKeyName: Option[String]) : H2OFrame = {
    import org.apache.spark.h2o.H2OProductUtils._
    import org.apache.spark.h2o.ReflectionUtils._

    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand()) // There are uniq IDs for RDD
    val fnames = names[A]
    val ftypes = types[A](fnames)
    // Collect H2O vector types for all input types
    val vecTypes = ftypes.indices.map(idx => dataTypeToVecType(ftypes(idx))).toArray
    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, fnames)
    // Create chunks on remote nodes
    val rows = sc.runJob(rdd, perRDDPartition(keyName, vecTypes) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.length)
    rows.foreach{ case(cidx, nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new H2OFrame(finalizeFrame(keyName, res, vecTypes))
  }

  /** Transform RDD[Primitive type] ( where primitive type can be String, Double, Float or Int) to appropriate H2OFrame */
  def toH2OFrame(sc: SparkContext, primitive: PrimitiveType, frameKeyName: Option[String]): H2OFrame = primitive.toH2OFrame(sc, frameKeyName)

  /** Transform RDD[String] to appropriate H2OFrame */
  def toH2OFrameFromRDDString(sc: SparkContext, rdd: RDD[String], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitive(sc, rdd, frameKeyName)

  /** Transform RDD[Int] to appropriate H2OFrame */
  def toH2OFrameFromRDDInt(sc: SparkContext, rdd: RDD[Int], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitive(sc, rdd, frameKeyName)

  /** Transform RDD[Float] to appropriate H2OFrame */
  def toH2OFrameFromRDDFloat(sc: SparkContext, rdd: RDD[Float], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitive(sc, rdd, frameKeyName)

  /** Transform RDD[Double] to appropriate H2OFrame */
  def toH2OFrameFromRDDDouble(sc: SparkContext, rdd: RDD[Double], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitive(sc, rdd, frameKeyName)

  /** Transform RDD[Long] to appropriate H2OFrame */
  def toH2OFrameFromRDDLong(sc: SparkContext, rdd: RDD[Long], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitive(sc, rdd, frameKeyName)

  /** Transform RDD[Double] to appropriate H2OFrame */
  def toH2OFrameFromRDDBool(sc: SparkContext, rdd: RDD[Boolean], frameKeyName: Option[String]): H2OFrame = toH2OFrameFromPrimitive(sc, rdd, frameKeyName)

  private[this]
  def toH2OFrameFromPrimitive[T: TypeTag](sc: SparkContext, rdd: RDD[T], frameKeyName: Option[String]): H2OFrame = {
    import org.apache.spark.h2o.H2OPrimitiveTypesUtils._
    import org.apache.spark.h2o.ReflectionUtils._

    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand())

    val fnames = Array[String]("values")
    val ftypes = Array[Class[_]](typ(typeOf[T]))
    val vecTypes = ftypes.indices.map(idx => dataTypeToVecType(ftypes(idx))).toArray

    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, fnames)

    val rows = sc.runJob(rdd, perPrimitivePartition(keyName, vecTypes) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.length)
    rows.foreach { case (cidx, nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new H2OFrame(finalizeFrame(keyName, res, vecTypes))
  }

  private
  def perPrimitivePartition[T](keystr: String, vecTypes: Array[Byte])
                              (context: TaskContext, it: Iterator[T]): (Int, Long) = {
    // An array of H2O NewChunks; A place to record all the data in this partition
    val nchks = water.fvec.FrameUtils.createNewChunks(keystr, vecTypes, context.partitionId)
    // Helper to hold H2O string
    val valStr = new BufferedString()
    it.foreach(r => {
      // For all rows in RDD
      val chk = nchks(0) // There is only one chunk
      r match {
        case t: Int => chk.addNum(t.toDouble)
        case t: Double => chk.addNum(t)
        case t: Float => chk.addNum(t.toDouble)
        case t: Long => chk.addNum(t.toDouble)
        case t: Boolean => chk.addNum(if(t) 1 else 0)
        case str: String => chk.addStr(valStr.setTo(str))
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
  def perRDDPartition[A<:Product](keystr:String, vecTypes: Array[Byte])
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
  def initFrame[T](keyName: String, names: Array[String]):Unit = {
    val fr = new water.fvec.Frame(Key.make(keyName))
    water.fvec.FrameUtils.preparePartialFrame(fr, names)
    // Save it directly to DKV
    fr.update(null)
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

  private
  def checkAndUpdateSparkEnv(conf: SparkConf): Unit = {
    // If 'spark.executor.instances' is specified update H2O property as well
    conf.getOption("spark.executor.instances").foreach(v => conf.set("spark.ext.h2o.cluster.size", v))
    // Increase locality timeout since h2o-specific tasks can be long computing
    if (conf.getInt("spark.locality.wait",3000) <= 3000) {
      logWarning(s"Increasing 'spark.locality.wait' to value 30000")
      conf.set("spark.locality.wait", "30000")
    }
  }

  private[h2o] def registerClientWebAPI(sc: SparkContext, h2OContext: H2OContext): Unit = {
    registerScalaIntEndp(sc, h2OContext)
    registerDataFramesEndp(sc, h2OContext)
    registerH2OFramesEndp(sc, h2OContext)
    registerRDDsEndp(sc)
  }

  private def registerH2OFramesEndp(sc: SparkContext, h2OContext: H2OContext) = {

    val h2OFramesHandler = new H2OFramesHandler(sc, h2OContext)

    def h2OFramesFactory = new HandlerFactory {
      override def create(handler: Class[_ <: Handler]): Handler = h2OFramesHandler
    }

    RequestServer.register("/3/h2oframes/(?<h2oframe_id>.*)/dataframe", "POST",
                           classOf[H2OFramesHandler], "toDataFrame",
                           null,
                           "Transform H2OFrame with given id to DataFrame",
                           h2OFramesFactory);

  }

  private def registerRDDsEndp(sc: SparkContext) = {

    val rddsHandler = new RDDsHandler(sc)

    def rddsFactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = rddsHandler
    }

    RequestServer.register("/3/RDDs", "GET",
                           classOf[RDDsHandler], "list",
                           null,
                           "Return all Frames in the H2O distributed K/V store.",
                           rddsFactory)

    RequestServer.register("/3/RDDs/(?<searched_rdd_id>[0-9]+)", "POST",
                           classOf[RDDsHandler], "getRDD",
                           null,
                           "Get frame in the H2O distributed K/V store with the given ID",
                           rddsFactory)

  }

  private def registerDataFramesEndp(sc: SparkContext, h2OContext: H2OContext) = {

    val dataFramesHandler = new DataFramesHandler(sc, h2OContext)

    def dataFramesfactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = dataFramesHandler
    }

    RequestServer.register("/3/dataframes", "GET",
                           classOf[DataFramesHandler], "list",
                           null,
                           "Return all DataFrames.",
                           dataFramesfactory)

    RequestServer.register("/3/dataframes/(?<searched_dataframe_id>[0-9a-zA-Z_]+)", "POST",
                           classOf[DataFramesHandler], "getDataFrame",
                           null,
                           "Get DataFrame with the given id",
                           dataFramesfactory)

    RequestServer.register("/3/dataframes/(?<dataframe_id>[0-9a-zA-Z_]+)/h2oframe", "POST",
                           classOf[DataFramesHandler], "toH2OFrame",
                           null,
                           "Transform DataFrame with the given id to H2OFrame",
                           dataFramesfactory)

  }

  private def registerScalaIntEndp(sc: SparkContext, h2OContext: H2OContext) = {
    val scalaCodeHandler = new ScalaCodeHandler(sc, h2OContext)
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
