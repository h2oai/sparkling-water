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

import java.io.File

import com.google.common.io.Files
import org.apache.spark.rdd.H2ORDD
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{Row, SchemaRDD}
import org.apache.spark._
import water.fvec.Vec
import water.parser.ValueString
import water._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark.h2o.H2OContextUtils._

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
  implicit def createDataFrameKey(rdd : SchemaRDD) : Key = toDataFrame(rdd)._key

  /** Implicit conversion from typed RDD to H2O's DataFrame */
  implicit def createDataFrameKey[A <: Product : TypeTag](rdd : RDD[A]) : Key
                                  = toDataFrame(rdd)._key

  /** Implicit conversion from Frame to DataFrame */
  implicit def createDataFrame(fr: Frame) : DataFrame = new DataFrame(fr)

  implicit def dataFrameToKey(fr: Frame): Key = fr._key

  implicit def symbolToString(sy: scala.Symbol): String = sy.name

  def toDataFrame(rdd: SchemaRDD) : DataFrame = H2OContext.toDataFrame(sparkContext, rdd)

  def toDataFrame[A <: Product : TypeTag](rdd: RDD[A]) : DataFrame
                                  = H2OContext.toDataFrame(sparkContext, rdd)

  def toRDD[A <: Product: TypeTag: ClassTag]( fr : DataFrame ) : RDD[A] = new H2ORDD[A](this,fr)



  /** Initialize Sparkling H2O and start H2O cloud with specified number of workers. */
  def start(h2oWorkers: Int):H2OContext = {
    sparkConf.set(PROP_CLUSTER_SIZE._1, h2oWorkers.toString)
    start()
  }

  /** Initialize Sparkling H2O and start H2O cloud. */
  def start(): H2OContext = {

    logInfo(s"Starting ${numH2OWorkers} H2O nodes...")
    // Create dummy RDD distributed over executors
    val (spreadRDD, nodes) = createSpreadRDD(numRddRetries, drddMulFactor, numH2OWorkers)

    // FIXME put here handling flatfile
    // Start H2O nodes
    // Get executors to execute H2O
    val allExecutorIds = nodes.map(_._1).distinct
    val executorIds = if (numH2OWorkers > 0) allExecutorIds.take(numH2OWorkers) else allExecutorIds
    if (executorIds.length < allExecutorIds.length) {
      logWarning(s"Spark cluster contains ${allExecutorIds.length}, but H2O is running only on ${executorIds} nodes!")
    }
    // Execute H2O on given nodes
    val executorStatus = startH2O(sparkContext, spreadRDD, executorIds, getH2OArgs() )
    // Verify that all specified executors contain running H2O
    if (!executorStatus.forall(x => !executorIds.contains(x._1) || x._2)) {
      throw new IllegalArgumentException(
        s"""Cannot execute H2O on all Spark executors:
           |  numH2OWorkers = ${numH2OWorkers}"
           |  executorStatus = ${executorStatus.mkString(",")}""".stripMargin)
    }
    logInfo("Sparkling H2O - H2O status: " + executorStatus.mkString(","))

    // Now connect to a cluster via H2O client,
    // but only in non-local case
    if (!sparkContext.isLocal) {
      logTrace("Sparkling H2O - DISTRIBUTED mode: Waiting for " + numH2OWorkers)
      H2OClientApp.main(getH2OArgs())
      H2O.finalizeRequest()
      H2O.waitForCloudSize(executorIds.length, cloudTimeout)
    } else {
      logTrace("Sparkling H2O - LOCAL mode")
      // Since LocalBackend does not wait for initialization (yet)
      H2O.waitForCloudSize(1, cloudTimeout)
    }

    this
  }


  /* Save flatfile and distribute it over cluster. */
  private def distributedFlatFile(nodes: Array[NodeDesc]):File = {
    // Create a flatfile in a temporary directory and distribute it around cluster
    val tmpDir = Files.createTempDir()
    tmpDir.deleteOnExit()
    val flatFile = new File(tmpDir, "flatfile.txt")
    // Save flatfile
    scala.tools.nsc.io.File(flatFile).writeAll(
      nodes.map(x => x._2 + ":" + x._3).mkString("", "\n","\n"))
    // Distribute the file around the Spark cluster via Spark infrastructure
    // - the file will be fetched by Executors
    sparkContext.addFile(flatFile.getAbsolutePath)
    logTrace("Sparkling H2O flatfile is " + flatFile.getAbsolutePath)
    flatFile
  }

  @tailrec
  private def createSpreadRDD(nretries:Int,
                              mfactor: Int,
                              nworkers: Int): (RDD[Int], Array[NodeDesc]) = {
    logDebug(s"  Creating RDD for launching H2O nodes (mretries=${nretries}, mfactor=${mfactor}, nworkers=${nworkers}")
    // Non-positive value means automatic detection of number of workers
    val workers = if (nworkers > 0) nworkers else defaultCloudSize
    val spreadRDD =
      sparkContext.parallelize(0 until mfactor*workers,
        mfactor*workers).persist()

    // Collect information about executors in Spark cluster
    val nodes = collectNodesInfo(spreadRDD, basePort, incrPort)

    // Verify that all executors participate in execution
    val sparkExecutors = nodes.map(_._1).distinct.length
    if (sparkExecutors < nworkers && nretries == 0) {
      throw new IllegalArgumentException(
        s"""Cannot execute H2O on all Spark executors: ${nodes.mkString(",")}
             |Expected number of h2o workers is ${nworkers}
             |Detected number of Spark workers is $sparkExecutors
             |Try to increase value in property ${PROP_DUMMY_RDD_MUL_FACTOR}
             |(its value is currently: ${mfactor} increased to ${mfactor})
             |""".stripMargin
      )
    } else if (sparkExecutors >= nworkers) {
      logInfo(s"Detected ${sparkExecutors} spark executors for ${nworkers} H2O workers!")
      (spreadRDD, nodes)
    } else {
      spreadRDD.unpersist()
      logInfo(s"Detected ${sparkExecutors} spark executors for ${nworkers} H2O workers! Retrying again...")
      createSpreadRDD(nretries-1, mfactor*2, nworkers)
    }
  }
}

object H2OContext {

  /** Transform SchemaRDD into H2O DataFrame */
  def toDataFrame(sc: SparkContext, rdd: SchemaRDD) : DataFrame = {
    val keyName = "frame_rdd_" + rdd.id + Key.rand() // There are uniq IDs for RDD
    val fnames = rdd.schema.fieldNames.toArray
    val ftypes = rdd.schema.fields.map( field => dataTypeToClass(field.dataType) ).toArray

    // Collect domains for String columns
    val fdomains = collectColumnDomains(sc, rdd, fnames, ftypes)

    initFrame(keyName, fnames)

    // Eager, not lazy, evaluation
    val rows = sc.runJob(rdd, perSQLPartition(keyName, ftypes, fdomains) _)
    val res = new Array[Long](rdd.partitions.size)
    rows.foreach { case(cidx,nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new DataFrame(finalizeFrame(keyName, res, ftypes, fdomains))
  }

  private def initFrame[T](keyName: String, names: Array[String]):Unit = {
    val fr = new water.fvec.Frame(Key.make(keyName))
    water.fvec.FrameUtils.preparePartialFrame(fr, names)
    // Save it directly to DKV
    fr.update(null)
  }
  private def finalizeFrame[T](keyName: String,
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

  private def translateToH2OType(t: Class[_], d: Array[String]):Byte = {
    t match {
      case q if q==classOf[java.lang.Short]   => Vec.T_NUM
      case q if q==classOf[java.lang.Integer] => Vec.T_NUM
      case q if q==classOf[java.lang.Long]    => Vec.T_NUM
      case q if q==classOf[java.lang.Float]   => Vec.T_NUM
      case q if q==classOf[java.lang.Double]  => Vec.T_NUM
      case q if q==classOf[java.lang.Boolean] => Vec.T_NUM
      case q if q==classOf[java.lang.String]  => if (d.length < water.parser.Enum.MAX_ENUM_SIZE) {
                                                    Vec.T_ENUM
                                                  } else {
                                                    Vec.T_STR
                                                  }
      case _ => !!!
    }
  }

  /** Transform typed RDD into H2O DataFrame */
  def toDataFrame[A <: Product : TypeTag](sc: SparkContext, rdd: RDD[A]) : DataFrame = {
    import org.apache.spark.h2o.ReflectionUtils._
    val keyName = "frame_rdd_" + rdd.id + Key.rand() // There are uniq IDs for RDD
    val fnames = names[A]
    val ftypes = types[A]
    // Collect domains for string columns
    val fdomains = collectColumnDomains(sc, rdd, fnames, ftypes)

    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, fnames)

    val rows = sc.runJob(rdd, perRDDPartition(keyName) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.size)
    rows.foreach{ case(cidx,nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new DataFrame(finalizeFrame(keyName, res, types, fdomains))
  }

  private def dataTypeToClass(dt : DataType):Class[_] = dt match {
    case BinaryType  => classOf[java.lang.Integer]
    case IntegerType => classOf[java.lang.Integer]
    case LongType    => classOf[java.lang.Long]
    case FloatType   => classOf[java.lang.Float]
    case DoubleType  => classOf[java.lang.Double]
    case StringType  => classOf[String]
    case BooleanType => classOf[java.lang.Boolean]
    case _ => throw new IllegalArgumentException(s"Unsupported type $dt")
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
          case q if q==classOf[Integer]           => nchk.addNum(row.getInt(i))
          case q if q==classOf[java.lang.Long]    => nchk.addNum(row.getLong(i))
          case q if q==classOf[java.lang.Double]  => nchk.addNum(row.getDouble(i))
          case q if q==classOf[java.lang.Float]   => nchk.addNum(row.getFloat(i))
          case q if q==classOf[java.lang.Boolean] => nchk.addNum(if (row.getBoolean(i)) 1 else 0)
          case q if q==classOf[String]            =>
            // too large domain - use String instead
            if (domains(i)==null) nchk.addStr(valStr.setTo(row.getString(i)))
            else {
              val sv = row.getString(i)
              val smap = domHash(i)
              nchk.addEnum(smap.getOrElse(sv, !!!))
            }
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

  private def collectColumnDomains(sc: SparkContext,
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
      rdd.foreach( r => { acc += r.productElement(idx).asInstanceOf[String] })
      res(idx) = acc.value.toArray.sorted
    }
    res
  }

  private def !!! = throw new IllegalArgumentException
  private def !!!(msg: => String) = throw new IllegalArgumentException(msg)

  def toRDD[A <: Product: TypeTag: ClassTag]
           ( h2oContext : H2OContext, fr : DataFrame ) : RDD[A] = new H2ORDD[A](h2oContext,fr)

}
