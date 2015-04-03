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

import org.apache.spark._
import org.apache.spark.h2o.H2OContextUtils._
import org.apache.spark.rdd.{H2ORDD, H2OSchemaRDD}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.{LogicalRDD, ExistingRdd, SparkLogicalPlan}
import org.apache.spark.sql.{Row, SQLContext, SchemaRDD}
import water._
import water.api._
import water.fvec.Vec
import water.parser.{Categorical, ValueString}

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
class H2OContext (@transient val sparkContext: SparkContext) extends {
    val sparkConf = sparkContext.getConf
  } with org.apache.spark.Logging
  with H2OConf
  with Serializable {

  /** Implicit conversion from SchemaRDD to H2O's DataFrame */
  implicit def createDataFrame(rdd : SchemaRDD) : DataFrame = toDataFrame(rdd)

  /** Implicit conversion from typed RDD to H2O's DataFrame */
  implicit def createDataFrame[A <: Product : TypeTag](rdd : RDD[A]) : DataFrame = toDataFrame(rdd)

  /** Implicit conversion from SchemaRDD to H2O's DataFrame */
  implicit def createDataFrameKey(rdd : SchemaRDD) : Key[Frame] = toDataFrame(rdd)._key

  /** Implicit conversion from typed RDD to H2O's DataFrame */
  implicit def createDataFrameKey[A <: Product : TypeTag](rdd : RDD[A]) : Key[_]
                                  = toDataFrame(rdd)._key

  /** Implicit conversion from Frame to DataFrame */
  implicit def createDataFrame(fr: Frame) : DataFrame = new DataFrame(fr)

  implicit def dataFrameToKey(fr: Frame): Key[Frame] = fr._key

  implicit def symbolToString(sy: scala.Symbol): String = sy.name

  def toDataFrame(rdd: SchemaRDD) : DataFrame = H2OContext.toDataFrame(sparkContext, rdd)

  def toDataFrame[A <: Product : TypeTag](rdd: RDD[A]) : DataFrame
                                  = H2OContext.toDataFrame(sparkContext, rdd)

  /** Convert given H2O frame into a RDD type */
  @deprecated("Use asRDD instead", "0.2.3")
  def toRDD[A <: Product: TypeTag: ClassTag](fr : DataFrame) : RDD[A] = asRDD[A](fr)

  /** Convert given H2O frame into a Product RDD type */
  def asRDD[A <: Product: TypeTag: ClassTag](fr : DataFrame) : RDD[A] = createH2ORDD[A](fr)

  /** Convert given H2O frame into SchemaRDD type */
  def asSchemaRDD(fr : DataFrame)(implicit sqlContext: SQLContext) : SchemaRDD = createH2OSchemaRDD(fr)

  /** Runtime list of nodes */
  private val h2oNodes = mutable.ArrayBuffer.empty[NodeDesc]
  /** Detected number of Spark executors
    * Property value is derived from SparkContext during creation of H2OContext. */
  private def numOfSparkExecutors = if (sparkContext.isLocal) 1 else sparkContext.getExecutorStorageStatus.length - 1

  private var localClient:String = _
  def h2oLocalClient = this.localClient
  def sparkUI = sparkContext.ui.map(ui => ui.appUIAddress)

  /** Initialize Sparkling H2O and start H2O cloud with specified number of workers. */
  def start(h2oWorkers: Int):H2OContext = {
    sparkConf.set(PROP_CLUSTER_SIZE._1, h2oWorkers.toString)
    start()
  }

  /** Initialize Sparkling H2O and start H2O cloud. */
  def start(): H2OContext = {
    // Setup properties for H2O configuration
    sparkConf.set(PROP_CLOUD_NAME._1,
      PROP_CLOUD_NAME._2 + System.getProperty("user.name", "cloud_" + Random.nextInt(42)))

    // Check Spark environment and reconfigure some values
    H2OContext.checkAndUpdateSparkEnv(sparkConf)

    logInfo(s"Starting H2O services: " + super[H2OConf].toString)
    // Create dummy RDD distributed over executors
    val (spreadRDD, spreadRDDNodes) = createSpreadRDD(numRddRetries, drddMulFactor, numH2OWorkers)

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

    val h2oNodeArgs = getH2ONodeArgs
    logDebug(s"Arguments used for launching h2o nodes: ${h2oNodeArgs.mkString(" ")}")
    val executors = startH2O(sparkContext, spreadRDD, spreadRDDNodes.length, this, h2oNodeArgs )
    // Store runtime information
    h2oNodes.append( executors:_* )

    // Now connect to a cluster via H2O client,
    // but only in non-local case
    // - get IP of local node
    if (!sparkContext.isLocal) {
      logTrace("Sparkling H2O - DISTRIBUTED mode: Waiting for " + executors.length)
      // Get arguments for this launch including flatfile
      val h2oClientArgs = toH2OArgs(getH2OClientArgs ++ Array("-ip", getIp(SparkEnv.get)),
                              this,
                              executors)
      logDebug(s"Arguments used for launching h2o nodes: ${h2oClientArgs.mkString(" ")}")
      H2OClientApp.main(h2oClientArgs)
      H2OContext.registerClientWebAPI(sparkContext)
      H2O.finalizeRegistration()
      H2O.waitForCloudSize(executors.length, cloudTimeout)
    } else {
      logTrace("Sparkling H2O - LOCAL mode")
      // Since LocalBackend does not wait for initialization (yet)
      H2O.waitForCloudSize(1, cloudTimeout)
    }
    // Get H2O web port
    localClient = H2O.getIpPortString
    // Inform user about status
    logInfo("Sparkling Water started, status of context: " + this.toString)
    this
  }

  @tailrec
  private
  def createSpreadRDD(nretries:Int,
                      mfactor: Int,
                      nworkers: Int): (RDD[NodeDesc], Array[NodeDesc]) = {
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
            | via ${PROP_CLUSTER_SIZE} Spark configuration property.
            | If you are running from shell,
            | you can try: val h2oContext = new H2OContext().start(<number of Spark workers>)
            |
            |""".stripMargin
      )
    } else if (nSparkExecAfter != nSparkExecBefore) {
      // Repeat if we detect change in number of executors reported by storage level
      logInfo(s"Detected ${nSparkExecBefore} before, and ${nSparkExecAfter} spark executors after! Retrying again...")
      createSpreadRDD(nretries-1, mfactor, nworkers)
    } else if ((nworkers>0 && sparkExecutors == nworkers || nworkers<=0) && sparkExecutors == nSparkExecAfter) {
      // Return result only if we are sure that number of detected executors seems ok
      logInfo(s"Detected ${sparkExecutors} spark executors for ${nworkers} H2O workers!")
      (new InvokeOnNodesRDD(nodes, sparkContext), nodes)
    } else {
      logInfo(s"Detected ${sparkExecutors} spark executors for ${nworkers} H2O workers! Retrying again...")
      createSpreadRDD(nretries-1, mfactor*2, nworkers)
    }
  }

  def createH2ORDD[A <: Product: TypeTag: ClassTag](fr: DataFrame): RDD[A] = {
    new H2ORDD[A](this,fr)
  }

  def createH2OSchemaRDD(fr: DataFrame)(implicit sqlContext: SQLContext):SchemaRDD = {
    //SparkPlan.currentContext.set(sqlContext)
    val h2oSchemaRDD = new H2OSchemaRDD(this, fr)
    val schemaAtts = H2OSchemaUtils.createSchema(fr).fields.map( f =>
      AttributeReference(f.name, f.dataType, f.nullable)())

    new SchemaRDD(sqlContext, LogicalRDD(schemaAtts, h2oSchemaRDD)(sqlContext))
  }

  /** Open H2O Flow running in this client. */
  def openFlow(): Unit = openURI(s"http://${h2oLocalClient}")
  /** Open Spark task manager. */
  def openSparkUI(): Unit = sparkUI.foreach(openURI(_))

  /** Open browser for given address.
    *
    * @param uri addres to open in browser, e.g., http://example.com
    */
  private def openURI(uri: String): Unit = {
    import java.awt.Desktop
    if (Desktop.isDesktopSupported) {
      Desktop.getDesktop.browse(new java.net.URI(uri))
    } else {
      logError(s"Desktop support is missing! Cannot open browser for ${uri}")
    }
  }

  override def toString: String = {
    s"""
      |Sparkling Water Context:
      | * number of executors: ${h2oNodes.size}
      | * list of used executors:
      |  (executorId, host, port)
      |  ------------------------
      |  ${h2oNodes.mkString("\n  ")}
      |  ------------------------
      |
      |  Open H2O Flow in browser: http://${localClient} (CMD + click in Mac OSX)
    """.stripMargin
  }
}

object H2OContext extends Logging {

  /** Transform SchemaRDD into H2O DataFrame */
  def toDataFrame(sc: SparkContext, rdd: SchemaRDD) : DataFrame = {
    import org.apache.spark.h2o.H2OSchemaUtils._

    val keyName = "frame_rdd_" + rdd.id //+ Key.rand() // There are uniq IDs for RDD
    // Fetch cached frame from DKV
    val frameVal = DKV.get(keyName)
    if (frameVal==null) {
      // Fetch information about SchemaRDD - string domains, max array length
      // Collect domains only for String columns
      val stringColIdxs = collectStringTypesIndx(rdd.schema.fields)
      // Contains only string domains of String columns and String arrays
      val fdomains      = collectColumnDomains(sc, rdd, stringColIdxs)

      // Flattens and expands RDD's schema
      val flatRddSchema = expandedSchema(sc, rdd)
      // Patch the flat schema based on information about types
      val fnames = flatRddSchema.map(t => t._2.name).toArray
      initFrame(keyName, fnames)

      // Eager, not lazy, evaluation
      val rows = sc.runJob(rdd, perSQLPartition(keyName, flatRddSchema, fdomains) _)
      val res = new Array[Long](rdd.partitions.size)
      rows.foreach { case (cidx, nrows) => res(cidx) = nrows}

      // Transform datatype into h2o types and expand string domains
      var strColCnt = 0
      val fH2ODomains = new Array[Array[String]](flatRddSchema.length)
      val ftypes = flatRddSchema.indices
        .map(idx => {
        val f = flatRddSchema(idx)
        f._2.dataType match {
          case StringType => {
            if (f._3 != VEC_TYPE && f._3 != ARRAY_TYPE) {
              val domain = fdomains(strColCnt)
              val result = dataTypeToVecType(f._2.dataType, domain)
              fH2ODomains(idx) = domain
              strColCnt += 1
              result
            } else Vec.T_STR
          }
          case _ => dataTypeToVecType(f._2.dataType, null)
        }
      }).toArray
      // Expand string domains into list for each column

      // Add Vec headers per-Chunk, and finalize the H2O Frame
      new DataFrame(
        finalizeFrame(
          keyName,
          res,
          ftypes,
          fH2ODomains))
    } else {
      new DataFrame(frameVal.get.asInstanceOf[Frame])
    }
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
                       colDomains: Array[Array[String]]):Frame = {
    val fr:Frame = DKV.get(keyName).get.asInstanceOf[Frame]
    val colH2OTypes = colTypes.indices.map(idx => {
      val typ = colTypes(idx)
      if (typ==Vec.T_STR) colDomains(idx) = null // minor clean-up
      typ
    }).toArray
    water.fvec.FrameUtils.finalizePartialFrame(fr, res, colDomains, colH2OTypes)
    fr
  }

  /** Transform typed RDD into H2O DataFrame */
  def toDataFrame[A <: Product : TypeTag](sc: SparkContext, rdd: RDD[A]) : DataFrame = {
    import org.apache.spark.h2o.ReflectionUtils._
    import org.apache.spark.h2o.H2OProductUtils._

    val keyName = "frame_rdd_" + rdd.id + Key.rand() // There are uniq IDs for RDD
    val fnames = names[A]
    val ftypes = types[A](fnames)
    // Collect domains for string columns
    val fdomains = collectColumnDomains(sc, rdd, fnames, ftypes)

    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, fnames)

    val rows = sc.runJob(rdd, perRDDPartition(keyName, fdomains) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.length)
    rows.foreach{ case(cidx,nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    val h2oTypes = ftypes.indices.map(idx => dataTypeToVecType(ftypes(idx), fdomains(idx))).toArray
    new DataFrame(finalizeFrame(keyName, res, h2oTypes, fdomains))
  }

  private
  def perSQLPartition ( keystr: String, types: Seq[(Seq[Byte], StructField, Byte)], domains: Array[Array[String]] )
                      ( context: TaskContext, it: Iterator[Row] ): (Int,Long) = {
    val nchks = water.fvec.FrameUtils.createNewChunks(keystr,context.partitionId)
    val domHash = domains.map( ary =>
      if (ary==null) {
        null.asInstanceOf[mutable.Map[String,Int]]
      } else {
        val m = new mutable.HashMap[String, Int]()
        for (idx <- ary.indices) m.put(ary(idx), idx)
        m
      })
    val valStr = new ValueString() // just helper for string columns
    it.foreach(row => {
      var numOfStringCols = 0
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
        while (i < path.length-1 && !subRow.isNullAt(path(i))) { subRow = subRow.getAs[Row](path(i)); i += 1 }
        val aidx = path(i) // actual index into row provided by path
        if (subRow.isNullAt(aidx)) chk.addNA()
        else {
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
            case DoubleType => chk.addNum(if (isAry)
              ary(aryIdx).asInstanceOf[Double]
              else if (isVec) subRow.getAs[mllib.linalg.Vector](aidx)(idx - startOfSeq)
              else subRow.getDouble(aidx))
            case StringType => {
              val sv = if (isAry) ary(aryIdx).asInstanceOf[String] else subRow.getString(aidx)
              if (isAry || domains(numOfStringCols) == null) {
                // String in arrays transform into string columns
                chk.addStr(valStr.setTo(sv))
              } else {
                val smap = domHash(numOfStringCols)
                chk.addEnum(smap.get(sv).get)
              }
              // Domains are computed only for regular string columns, not for Array[String]
              if (!isAry) numOfStringCols += 1
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
  def perRDDPartition[A<:Product]( keystr:String, domains: Array[Array[String]] )
                                         ( context: TaskContext, it: Iterator[A] ): (Int,Long) = {
    // An array of H2O NewChunks; A place to record all the data in this partition
    val nchks = water.fvec.FrameUtils.createNewChunks(keystr,context.partitionId)
    // Make a helper hash for enum domains
    val domHash = domains.map( ary =>
      if (ary==null) {
        null.asInstanceOf[mutable.Map[String,Int]]
      } else {
        val m = new mutable.HashMap[String, Int]()
        for (idx <- ary.indices) m.put(ary(idx), idx)
        m
      })

    val valStr = new ValueString()
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
          case n: String  =>
            if (domains(i)==null) chk.addStr(valStr.setTo(n))
            else {
              val sv = n
              val smap = domHash(i)
              chk.addEnum(smap.get(sv).get)
            }
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
  def checkAndUpdateSparkEnv(conf: SparkConf): Unit = {
    // If 'spark.executor.instances' is specified update H2O property as well
    conf.getOption("spark.executor.instances").foreach(v => conf.set("spark.ext.h2o.cluster.size", v))
    // Increase locality timeout since h2o-specific tasks can be long computing
    if (conf.getInt("spark.locality.wait",3000) <= 3000) {
      logWarning(s"Increasing 'spark.locality.wait' to value 30000")
      conf.set("spark.locality.wait", "30000")
    }
  }

  private[h2o] def registerClientWebAPI(sc: SparkContext): Unit = {
    def hfactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = new RDDsHandler(sc)
    }
    RequestServer.register("/3/RDDs", "GET",
                            classOf[RDDsHandler], "list",
                            null, new Array[String](0),
                            "Return all Frames in the H2O distributed K/V store.",
                            hfactory)
  }
}
