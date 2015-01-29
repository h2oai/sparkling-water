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
import org.apache.spark.scheduler.{SparkListenerBlockManagerAdded, SparkListenerBlockManagerRemoved}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.{ExistingRdd, SparkLogicalPlan}
import org.apache.spark.sql.{Row, SQLContext, SchemaRDD}
import water._
import water.fvec.Vec
import water.parser.ValueString

import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Random

/**
 * Simple H2O context motivated by SQLContext.
 *
 * Doing - implicit conversion from RDD -> H2OLikeRDD
 *
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

    // Check Spark environment
    H2OContext.checkSparkEnv(sparkConf)

    logInfo(s"Starting H2O services: " + super[H2OConf].toString)
    // Create dummy RDD distributed over executors
    val (spreadRDD, nodes) = createSpreadRDD(numRddRetries, drddMulFactor, numH2OWorkers)

    // Start H2O nodes
    // Get executors to execute H2O
    val allExecutorIds = nodes.map(_._1).distinct
    val executorIds = allExecutorIds
    val executors = nodes // Executors list should be already normalized
    // The collected executors based on IDs should match
    assert(executors.length == executorIds.length,
            s"Unexpected number of executors ${executors.length}!=${executorIds.length}")
    // H2O is executed only on the subset of Spark cluster
    // This situation is interesting when data are located on node which does not contain H2O
    if (executorIds.length < allExecutorIds.length) {
      logWarning(s"""Spark cluster contains ${allExecutorIds.length},
               but H2O is running only on ${executorIds.length} nodes!""")
    }
    // Execute H2O on given nodes
    logInfo(s"""Launching H2O on following nodes: ${executors.mkString(",")}""")

    val executorStatus = startH2O(sparkContext, spreadRDD, executors, this, getH2OArgs() )
    // Verify that all specified executors contain running H2O
    if (!executorStatus.forall(x => !executorIds.contains(x._1) || x._2)) {
      throw new IllegalArgumentException(
        s"""Cannot execute H2O on all Spark executors:
           |  numH2OWorkers = ${numH2OWorkers}"
           |  executorStatus = ${executorStatus.mkString(",")}""".stripMargin)
    }

    // Store runtime information
    h2oNodes.append( executors:_* )

    // Now connect to a cluster via H2O client,
    // but only in non-local case
    if (!sparkContext.isLocal) {
      logTrace("Sparkling H2O - DISTRIBUTED mode: Waiting for " + numH2OWorkers)
      // Get arguments for this launch including flatfile
      val h2oArgs = toH2OArgs(getH2OArgs() ++ Array("-ip", getIp(SparkEnv.get)),
                              this,
                              executors)
      H2OClientApp.main(h2oArgs)
      H2O.finalizeRegistration()
      H2O.waitForCloudSize(executors.length, cloudTimeout)
    } else {
      logTrace("Sparkling H2O - LOCAL mode")
      // Since LocalBackend does not wait for initialization (yet)
      H2O.waitForCloudSize(1, cloudTimeout)
    }

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
    val nodes = collectNodesInfo(spreadRDD, basePort, incrPort)

    // Verify that all executors participate in execution
    val nSparkExecAfter = numOfSparkExecutors
    val sparkExecutors = nodes.map(_._1).distinct.length
    // Delete RDD
    spreadRDD.unpersist()
    if ( (sparkExecutors < nworkers || nSparkExecAfter != nSparkExecBefore)
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
      createSpreadRDD(nretries-1, mfactor, nSparkExecAfter)
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

    new SchemaRDD(sqlContext,
      SparkLogicalPlan(
        ExistingRdd(
          schemaAtts, h2oSchemaRDD))(sqlContext))
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
    """.stripMargin
  }
}

object H2OContext extends Logging {

  /** Transform SchemaRDD into H2O DataFrame */
  def toDataFrame(sc: SparkContext, rdd: SchemaRDD) : DataFrame = {
    val keyName = "frame_rdd_" + rdd.id //+ Key.rand() // There are uniq IDs for RDD
    val frameVal = DKV.get(keyName)
    if (frameVal==null) {
      val fnames = rdd.schema.fieldNames.toArray
      val ftypes = rdd.schema.fields.map(field => dataTypeToClass(field.dataType)).toArray

      // Collect domains for String columns
      val fdomains = collectColumnDomains(sc, rdd, fnames, ftypes)

      initFrame(keyName, fnames)

      // Eager, not lazy, evaluation
      val rows = sc.runJob(rdd, perSQLPartition(keyName, ftypes, fdomains) _)
      val res = new Array[Long](rdd.partitions.size)
      rows.foreach { case (cidx, nrows) => res(cidx) = nrows}

      // Add Vec headers per-Chunk, and finalize the H2O Frame
      new DataFrame(finalizeFrame(keyName, res, ftypes, fdomains))
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
                               colTypes: Array[Class[_]],
                               colDomains: Array[Array[String]]):Frame = {
    val fr:Frame = DKV.get(keyName).get.asInstanceOf[Frame]
    val colH2OTypes = colTypes.indices.map(idx => {
      val typ = translateToH2OType(colTypes(idx), colDomains(idx))
      if (typ==Vec.T_STR) colDomains(idx) = null // minor clean-up
      typ
    }).toArray
    water.fvec.FrameUtils.finalizePartialFrame(fr, res, colDomains, colH2OTypes)
    fr
  }

  private
  def translateToH2OType(t: Class[_], d: Array[String]):Byte = {
    t match {
      case q if q==classOf[java.lang.Byte]    => Vec.T_NUM
      case q if q==classOf[java.lang.Short]   => Vec.T_NUM
      case q if q==classOf[java.lang.Integer] => Vec.T_NUM
      case q if q==classOf[java.lang.Long]    => Vec.T_NUM
      case q if q==classOf[java.lang.Float]   => Vec.T_NUM
      case q if q==classOf[java.lang.Double]  => Vec.T_NUM
      case q if q==classOf[java.lang.Boolean] => Vec.T_NUM
      case q if q==classOf[java.lang.String]  => if (d.length < water.parser.Categorical.MAX_ENUM_SIZE) {
                                                    Vec.T_ENUM
                                                  } else {
                                                    Vec.T_STR
                                                  }
      case q if q==classOf[java.sql.Timestamp] => Vec.T_TIME
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }

  /** Transform typed RDD into H2O DataFrame */
  def toDataFrame[A <: Product : TypeTag](sc: SparkContext, rdd: RDD[A]) : DataFrame = {
    import org.apache.spark.h2o.ReflectionUtils._
    val keyName = "frame_rdd_" + rdd.id + Key.rand() // There are uniq IDs for RDD
    val fnames = names[A]
    val ftypes = types[A](fnames)
    // Collect domains for string columns
    val fdomains = collectColumnDomains(sc, rdd, fnames, ftypes)

    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, fnames)

    val rows = sc.runJob(rdd, perRDDPartition(keyName) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.length)
    rows.foreach{ case(cidx,nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new DataFrame(finalizeFrame(keyName, res, ftypes, fdomains))
  }

  private
  def perSQLPartition ( keystr: String, types: Array[Class[_]], domains: Array[Array[String]] )
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
    it.foreach(row => {
      val valStr = new ValueString()
      for( i <- 0 until types.length) {
        val nchk = nchks(i)
        if (row.isNullAt(i)) nchk.addNA()
        else types(i) match {
          case q if q==classOf[java.lang.Byte]    => nchk.addNum(row.getByte  (i))
          case q if q==classOf[java.lang.Short]   => nchk.addNum(row.getShort (i))
          case q if q==classOf[Integer]           => nchk.addNum(row.getInt   (i))
          case q if q==classOf[java.lang.Long]    => nchk.addNum(row.getLong  (i))
          case q if q==classOf[java.lang.Double]  => nchk.addNum(row.getDouble(i))
          case q if q==classOf[java.lang.Float]   => nchk.addNum(row.getFloat (i))
          case q if q==classOf[java.lang.Boolean] => nchk.addNum(if (row.getBoolean(i)) 1 else 0)
          case q if q==classOf[String]            =>
            // too large domain - use String instead
            if (domains(i)==null) nchk.addStr(valStr.setTo(row.getString(i)))
            else {
              val sv = row.getString(i)
              val smap = domHash(i)
              nchk.addEnum(smap.getOrElse(sv, !!!))
            }
          case q if q==classOf[java.sql.Timestamp] => nchk.addNum(row.getAs[java.sql.Timestamp](i).getTime())
          case _ => Double.NaN
        }
      }
    })
    // Compress & write out the Partition/Chunks
    water.fvec.FrameUtils.closeNewChunks(nchks)
    // Return Partition# and rows in this Partition
    (context.partitionId,nchks(0)._len)
  }

  private
  def perRDDPartition[A<:Product]( keystr:String )
                                         ( context: TaskContext, it: Iterator[A] ): (Int,Long) = {
    // An array of H2O NewChunks; A place to record all the data in this partition
    val nchks = water.fvec.FrameUtils.createNewChunks(keystr,context.partitionId)
    it.foreach(prod => { // For all rows which are subtype of Product
      for( i <- 0 until prod.productArity ) { // For all fields...
      val fld = prod.productElement(i)
        nchks(i).addNum( { // Copy numeric data from fields to NewChunks
        val x = fld match { case Some(n) => n; case _ => fld }
          x match {
            case n: Number => n.doubleValue
            case n: Boolean => if (n) 1 else 0
            case _ => Double.NaN
          }
        } )
      }
    })
    // Compress & write out the Partition/Chunks
    water.fvec.FrameUtils.closeNewChunks(nchks)
    // Return Partition# and rows in this Partition
    (context.partitionId,nchks(0)._len)
  }

  private
  def collectColumnDomains(sc: SparkContext,
                           rdd: SchemaRDD,
                           fnames: Array[String],
                           ftypes: Array[Class[_]]): Array[Array[String]] = {
    val res = Array.ofDim[Array[String]](fnames.length)
    for (idx <- 0 until ftypes.length if ftypes(idx).equals(classOf[String])) {
      val acc =  sc.accumulableCollection(new mutable.HashSet[String]())
      // Distributed ops
      rdd.foreach( r => { acc += r.getString(idx) })
      res(idx) = acc.value.toArray.sorted
    }
    res
  }

  private def collectColumnDomains[A <: Product](sc: SparkContext,
                                   rdd: RDD[A],
                                   fnames: Array[String],
                                   ftypes: Array[Class[_]]): Array[Array[String]] = {
    val res = Array.ofDim[Array[String]](fnames.length)
    for (idx <- 0 until ftypes.length if ftypes(idx).equals(classOf[String])) {
      val acc =  sc.accumulableCollection(new mutable.HashSet[String]())
      // Distributed ops
      // FIXME product element can be Optional or Non-optional
      rdd.foreach( r => { acc += r.productElement(idx).asInstanceOf[Option[String]].get })
      res(idx) = acc.value.toArray.sorted
    }
    res
  }

  private def !!! = throw new IllegalArgumentException

  private
  def checkSparkEnv(conf: SparkConf): Unit = {
    if (conf.getInt("spark.locality.wait",3000) <= 3000) {
      logWarning(s"Increasing 'spark.locality.wait' to value 30000")
      conf.set("spark.locality.wait", "30000")
    }
  }
}

private[h2o]
trait SparkEnvListener extends org.apache.spark.scheduler.SparkListener { self: H2OContext =>

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    println("--------------------> onBlockManagerAdded: "+ blockManagerAdded)
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    println("--------------------> onBlockManagerRemoved: "+ blockManagerRemoved)
  }
}

