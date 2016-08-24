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
import org.apache.spark.h2o.backends.internal.InternalH2OBackend
import org.apache.spark.h2o.converters._
import org.apache.spark.h2o.utils.{H2OContextUtils, NodeDesc}
import org.apache.spark.sql.{DataFrame, SQLContext}
import water._
import water.parser.BufferedString

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
class H2OContext private (@transient val sparkContext: SparkContext, @transient conf: H2OConf, sqlc: SQLContext) extends org.apache.spark.Logging
  with Serializable with SparkDataFrameConverter with SupportedRDDConverter with H2OContextUtils{
  self =>

  // TODO(vlad): remove it asap, it's temporary
  //  /** Supports call from java environments. */
  //  def this(sparkContext: JavaSparkContext, @transient conf: H2OConf) = this(sparkContext.sc, conf)

  /** IP of H2O client */
  private var localClientIp: String = _
  /** REST port of H2O client */
  private var localClientPort: Int = _
  /** Runtime list of active H2O nodes */
  private val h2oNodes = mutable.ArrayBuffer.empty[NodeDesc]


  /** Used backend */
  @transient private val backend: SparklingBackend = new InternalH2OBackend(this)


  // Check Spark and H2O environment for general arguments independent on backend used and
  // also with regards to used backend and store the fix the state of prepared configuration
  // so it can't be changed anymore
  /** H2O and Spark configuration */
  @transient val _conf = backend.checkAndUpdateConf(conf).clone()

  /**
    * This method connects to external H2O cluster if spark.ext.h2o.externalClusterMode is set to true,
    * otherwise it creates new H2O cluster living in Spark
    */
  def init(): H2OContext = {
    if(!isRunningOnCorrectSpark(sparkContext)){
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
    logInfo("Sparkling Water started, status of context: " + this.toString)

    // Store this instance so it can be obtained using getOrCreate method
    H2OContext.setInstantiatedContext(this)
    this
  }

  /**
    * Return a copy of this H2OContext's configuration. The configuration ''cannot'' be changed at runtime.
    */
  def getConf: H2OConf = _conf.clone()

  /** Transforms RDD[Supported type] to H2OFrame */
  def asH2OFrame(rdd: SupportedRDD): H2OFrame = asH2OFrame(rdd, None)
  def asH2OFrame(rdd: SupportedRDD, frameName: Option[String]): H2OFrame =  toH2OFrame(this, rdd, frameName)
  def asH2OFrame(rdd: SupportedRDD, frameName: String): H2OFrame = asH2OFrame(rdd, Option(frameName))


  /** Transforms RDD[Supported type] to H2OFrame key */
  def toH2OFrameKey(rdd: SupportedRDD): Key[_] = toH2OFrameKey(rdd, None)
  def toH2OFrameKey(rdd: SupportedRDD, frameName: Option[String]): Key[_] = asH2OFrame(rdd, frameName)._key
  def toH2OFrameKey(rdd: SupportedRDD, frameName: String): Key[_] = toH2OFrameKey(rdd, Option(frameName))

  /** Transform DataFrame to H2OFrame */
  def asH2OFrame(df : DataFrame): H2OFrame = asH2OFrame(df, None)
  def asH2OFrame(df : DataFrame, frameName: Option[String]) : H2OFrame = toH2OFrame(this, df, frameName)
  def asH2OFrame(df : DataFrame, frameName: String) : H2OFrame = asH2OFrame(df, Option(frameName))
  /** Transforms Dataset[Supported type] to H2OFrame */
  def asH2OFrame[T<: Product : TypeTag](ds: Dataset[T]): H2OFrame = asH2OFrame(ds, None)
  def asH2OFrame[T<: Product : TypeTag](ds: Dataset[T], frameName: Option[String]): H2OFrame =
    ProductRDDConverter.toH2OFrame(self, ds.rdd, frameName)
  def asH2OFrame[T<: Product : TypeTag](ds: Dataset[T], frameName: String): H2OFrame = asH2OFrame(ds, Option(frameName))

  /** Transform DataFrame to H2OFrame key */
  def toH2OFrameKey(df : DataFrame): Key[Frame] = toH2OFrameKey(df, None)
  def toH2OFrameKey(df : DataFrame, frameName: Option[String]) : Key[Frame] = asH2OFrame(df, frameName)._key
  def toH2OFrameKey(df : DataFrame, frameName: String) : Key[Frame] = toH2OFrameKey(df, Option(frameName))

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
  def asRDD[A <: Product: TypeTag: ClassTag](fr : H2OFrame) : RDD[A] = toRDD[A, H2OFrame](this, fr)

  /** A generic convert of Frame into Product RDD type
    *
    * Consider using asH2OFrame since asRDD has several limitations such as that asRDD can't be used in Spark REPL
    * in case we are RDD[T] where T is class defined in REPL. This is because class T is created as inner class
    * and we are not able to create instance of class T without outer scope - which is impossible to get.
    *
    * This code: hc.asRDD[PUBDEV458Type](rdd) will need to be call as hc.asRDD[PUBDEV458Type].apply(rdd)
    */
  def asRDD[A <: Product : TypeTag : ClassTag] = new {
    def apply[T <: Frame](fr: T): RDD[A] = toRDD[A, T](H2OContext.this, fr)
  }

  /** Convert given H2O frame into DataFrame type */
  @deprecated("1.3", "Use asDataFrame")
  def asSchemaRDD[T <: Frame](fr : T, copyMetadata: Boolean = true)(implicit sqlContext: SQLContext) : DataFrame = toDataFrame(this, fr, copyMetadata)
  def asDataFrame[T <: Frame](fr : T, copyMetadata: Boolean = true)(implicit sqlContext: SQLContext) : DataFrame = toDataFrame(this, fr, copyMetadata)
  def asDataFrame(s : String, copyMetadata: Boolean)(implicit sqlContext: SQLContext) : DataFrame = toDataFrame(this, new H2OFrame(s), copyMetadata)

  def h2oLocalClient = this.localClientIp + ":" + this.localClientPort

  def h2oLocalClientIp = this.localClientIp

  def h2oLocalClientPort = this.localClientPort

  // For now disable opening Spark UI
  //def sparkUI = sparkContext.ui.map(ui => ui.appUIAddress)

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

  @transient private val instantiatedContext = new AtomicReference[H2OContext]()

  /**
    * Tries to get existing H2O Context. If it has been created, returns this H2O Context, otherwise
    * returns creates a new one
    *
    * @return H2OContext
    */
  def get(): Option[H2OContext] = Option(instantiatedContext.get())

  /**
    * Get existing or create new H2OContext based on provided H2O configuration
    *
    * @param sc Spark Context
    * @param conf H2O configuration
    * @return H2O Context
    */
  def getOrCreate(sc: SparkContext, conf: H2OConf)(implicit sqlContext: SQLContext): H2OContext = synchronized {
    // TODO(vlad): figure out how thread-safe are these operations IRL
    if (instantiatedContext.get() == null) {
      instantiatedContext.set(new H2OContext(sc, conf, sqlContext))
      instantiatedContext.get().init()
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
    getOrCreate(sc, new H2OConf(sc))(SQLContext.getOrCreate(sc))
  }

  val defaultFieldNames = (i: Int) => "f" + i

  def buildH2OFrame(kn: String, vecTypes: Array[Byte], res: Array[Long]): H2OFrame = {
    val u = this.asInstanceOf[ConverterUtils]
    new H2OFrame(u.finalizeFrame(kn, res, vecTypes))
  }

  case class MetaInfo(names:Array[String], vecTypes: Array[Byte]) {
    require(names.length > 0, "Empty meta info does not make sense")
    require(names.length == vecTypes.length, s"Different lengths: ${names.length} names, ${vecTypes.length} types")
  }

  private
  def initFrame[T](keyName: String, names: Array[String]):Unit = {
    val fr = new water.fvec.Frame(Key.make(keyName))
    water.fvec.FrameUtils.preparePartialFrame(fr, names)
    // Save it directly to DKV
    fr.update()
  }

  //  private
  //  def finalizeFrame[T](keyName: String,
  //                       res: Array[Long],
  //                       colTypes: Array[Byte],
  //                       colDomains: Array[Array[String]] = null):Frame = {
  //    val fr:Frame = DKV.get(keyName).get.asInstanceOf[Frame]
  //    water.fvec.FrameUtils.finalizePartialFrame(fr, res, colDomains, colTypes)
  //    fr
  //  }

  def keyName(rdd: RDD[_], frameKeyName: Option[String]) = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand())

  case class FromPureProduct(sc: SparkContext, rdd: RDD[Product], frameKeyName: Option[String]) {

    import scala.reflect.runtime.universe._

    def withDefaultFieldNames() = {
      withFieldNames(defaultFieldNames)
    }

    def withFieldNames(fieldNames: Int => String): H2OFrame = {
      val meta = metaInfo(fieldNames)
      withMeta(meta)
    }

    def withFields(tuples: List[(String, Type)]): H2OFrame = {
      val meta = metaInfo(tuples)
      withMeta(meta)
    }

    def withMeta(meta: MetaInfo): H2OFrame = {

      val kn: String = keyName(rdd, frameKeyName)
      // Make an H2O data Frame - but with no backing data (yet)
      initFrame(kn, meta.names)
      // Create chunks on remote nodes
      val rows = sc.runJob(rdd, partitionJob(kn, meta.vecTypes)) // eager, not lazy, evaluation
      val res = new Array[Long](rdd.partitions.length)
      rows.foreach { case (cidx, nrows) => res(cidx) = nrows }

      // Add Vec headers per-Chunk, and finalize the H2O Frame
      buildH2OFrame(kn, meta.vecTypes, res)
    }

    private
    def perTypedDataPartition[A<:Product](keystr:String, vecTypes: Array[Byte])
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
            case n: String  => chk.addStr(valStr.set(n))
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

    def partitionJob[A <: Product : TypeTag](keyName: String, vecTypes: Array[Byte]): (TaskContext, Iterator[Product]) => (Int, Long) = {
      perTypedDataPartition(keyName, vecTypes)
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

    import org.apache.spark.h2o.utils.H2OTypeUtils._

    def metaInfo(fieldNames: Int => String): MetaInfo = {
      val first = rdd.first()
      val fnames: Array[String] = (0 until first.productArity map fieldNames).toArray[String]

      val ftypes = first.productIterator map inferFieldType

      // Collect H2O vector types for all input types
      val vecTypes:Array[Byte] = ftypes map dataTypeToVecType toArray

      MetaInfo(fnames, vecTypes)
    }

    def metaInfo(tuples: List[(String, Type)]): MetaInfo = {
      val names = tuples map (_._1) toArray
      val vecTypes = tuples map (nt => dataTypeToVecType(nt._2)) toArray

      MetaInfo(names, vecTypes)
    }
  }

}

class WrongSparkVersion(msg: String) extends Exception(msg) with NoStackTrace

