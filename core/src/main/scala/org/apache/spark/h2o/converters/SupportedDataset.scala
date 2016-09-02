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

package org.apache.spark.h2o.converters

import org.apache.spark.h2o._
import org.apache.spark.h2o.utils.SupportedTypes.SupportedType
import org.apache.spark.h2o.utils._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, TaskContext}
import water.Key
import water.parser.BufferedString

import scala.language.{implicitConversions, postfixOps}
import scala.reflect.runtime.universe._

  /**
  * Magnet pattern (Type Class pattern) for conversion from various case classes to their appropriate H2OFrame using
  * the method with the same name
  */
trait SupportedDataset[T] {
    val typeTag: TypeTag[T]
  def toH2OFrame(sc: SQLContext, frameKeyName: Option[String]): H2OFrame
}

object SupportedDataset {

  implicit def toH2OFrameFromDataset[T <: Product](ds: Dataset[T])(implicit ttag:TypeTag[T]): SupportedDataset[T] = new SupportedDataset[T] {
    val typeTag = ttag
    override def toH2OFrame(sc: SQLContext, frameKeyName: Option[String]): H2OFrame = {
      val tpe = ttag.tpe
      val constructorSymbol = tpe.declaration(nme.CONSTRUCTOR)
      val defaultConstructor =
        if (constructorSymbol.isMethod) constructorSymbol.asMethod
        else {
          val ctors = constructorSymbol.asTerm.alternatives
          ctors.map { _.asMethod }.find { _.isPrimaryConstructor }.get
        }

      val params: List[(String, Type)] = defaultConstructor.paramss.flatten map {
        sym => sym.name.toString -> tpe.member(sym.name).asMethod.returnType
      }

      val fieldNames = ds.schema.fieldNames

      val rdd: RDD[Product] = try {
        ds.rdd.asInstanceOf[RDD[Product]]
      } catch {
        case oops: Exception =>
          oops.printStackTrace()
          throw oops
      }
      val res = try {

        val prototype = ProductFrameBuilder(sc.sparkContext, rdd, frameKeyName)
        prototype.withFields(params)
      } catch {
        case oops: Exception =>
          oops.printStackTrace()
          throw oops
      }
      res
    }
  }

  case class MetaInfo(names:Array[String], types: Array[SupportedType]) {
    require(names.length > 0, "Empty meta info does not make sense")
    require(names.length == types.length, s"Different lengths: ${names.length} names, ${types.length} types")
    lazy val vecTypes: Array[Byte] = types map (_.vecType)
  }

  case class ProductFrameBuilder(sc: SparkContext, rdd: RDD[Product], frameKeyName: Option[String]) {

    val defaultFieldNames = (i: Int) => "f" + i

    import scala.reflect.runtime.universe._

    def withDefaultFieldNames() = {
      withFieldNames(defaultFieldNames)
    }

    def withFieldNames(fieldNames: Int => String): H2OFrame = {
      val meta = metaInfo(fieldNames)
      withMeta(meta)
    }

    def withFields(fields: List[(String, Type)]): H2OFrame = {
      val meta = metaInfo(fields)
      withMeta(meta)
    }

    def keyName(rdd: RDD[_], frameKeyName: Option[String]) = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand())

    private
    def initFrame[T](keyName: String, names: Array[String]):Unit = {
      val fr = new water.fvec.Frame(Key.make(keyName))
      water.fvec.FrameUtils.preparePartialFrame(fr, names)
      // Save it directly to DKV
      fr.update()
    }

    private def buildH2OFrame(kn: String, vecTypes: Array[Byte], res: Array[Long]): H2OFrame = {
      new H2OFrame(ConverterUtils.finalizeFrame(kn, res, vecTypes))
    }

    def withMeta(meta: MetaInfo): H2OFrame = {

      val kn: String = keyName(rdd, frameKeyName)
      // Make an H2O data Frame - but with no backing data (yet)
      initFrame(kn, meta.names)
      // Create chunks on remote nodes
      val rows = sc.runJob(rdd, ProductFrameBuilder.partitionJob(kn, meta.vecTypes)) // eager, not lazy, evaluation
      val res = new Array[Long](rdd.partitions.length)
      rows.foreach { case (cidx, nrows) => res(cidx) = nrows }

      // Add Vec headers per-Chunk, and finalize the H2O Frame
      buildH2OFrame(kn, meta.vecTypes, res)
    }

    import ReflectionUtils._

    def metaInfo(fieldNames: Int => String): MetaInfo = {
      val first = rdd.first()
      val fnames: Array[String] = (0 until first.productArity map fieldNames).toArray[String]

      MetaInfo(fnames, memberTypes(first))
    }

    def metaInfo(tuples: List[(String, Type)]): MetaInfo = {
      val names = tuples map (_._1) toArray
      val vecTypes = tuples map (nt => supportedTypeFor(nt._2)) toArray

      MetaInfo(names, vecTypes)
    }
  }

  object ProductFrameBuilder {
    private def perTypedDataPartition[A<:Product](keystr:String, vecTypes: Array[Byte])
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

  }
}
