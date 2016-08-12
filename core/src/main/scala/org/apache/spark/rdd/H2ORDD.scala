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

package org.apache.spark.rdd

import language.postfixOps
import java.lang.reflect.Constructor

import org.apache.spark.h2o.{H2OContext, ReflectionUtils}
import org.apache.spark.{Partition, TaskContext}
import water.fvec.{Chunk, Frame}
import water.parser.BufferedString

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Convert H2OFrame into an RDD (lazily)
 */

private[spark]
class H2ORDD[A <: Product: TypeTag: ClassTag, T <: Frame] private(@transient val h2oContext: H2OContext,
                                                                  @transient val frame: T,
                                                                  val colNames: Array[String])
  extends RDD[A](h2oContext.sparkContext, Nil) with H2ORDDLike[T] {

  // Get column names before building an RDD
  def this(h2oContext: H2OContext, fr : T) = this(h2oContext,fr,ReflectionUtils.names[A])
  // Check that H2OFrame & given Scala type are compatible
  if (colNames.length > 1) {
    colNames.foreach { name =>
      if (frame.find(name) == -1) {
        throw new IllegalArgumentException("Scala type has field " + name +
          " but H2OFrame does not have a matching column; has " + frame.names().mkString(","))
      }
    }
  }

  private val columnTypeNames = ReflectionUtils.typeNames[A](colNames)

  private val jc = implicitly[ClassManifest[A]].runtimeClass

  private type Deserializer = (Chunk, Int) => Any

  private val DefaultDeserializer: Deserializer = (chk: Chunk, row: Int) => null

  def extractStringLike(chk: Chunk, row: Int): String = {
    if (chk.vec().isCategorical) {
      chk.vec().domain()(chk.at8(row).asInstanceOf[Int])
    } else if (chk.vec().isString) {
      val valStr = new BufferedString()
      chk.atStr(valStr, row)
      valStr.toString
//      // TODO(vlad): fix BufferedString, so it does not throw if buffer is null
//      if(valStr.getBuffer() == null) null else valStr.toString
    } else null
  }

  private val plainExtractors: Map[String, Deserializer] = Map(
    "Boolean" -> ((chk: Chunk, row: Int) => chk.at8(row) == 1),
    "Byte"    -> ((chk: Chunk, row: Int) => chk.at8(row).toByte),
    "Double"  -> ((chk: Chunk, row: Int) => chk.atd(row)),
    "Float"   -> ((chk: Chunk, row: Int) => chk.atd(row)),
    "Integer" -> ((chk: Chunk, row: Int) => chk.at8(row).asInstanceOf[Int]),
    "Long"    -> ((chk: Chunk, row: Int) => chk.at8(row)),
    "Short"   -> ((chk: Chunk, row: Int) => chk.at8(row).asInstanceOf[Short]),
    "String"  -> ((chk: Chunk, row: Int) => extractStringLike(chk, row)))

  private def returnOption[X](op: (Chunk, Int) => X) = (chk: Chunk, row: Int) => opt(op(chk, row))

  private val allExtractors: Map[String, Deserializer] = plainExtractors ++
    (plainExtractors map {case (key, op) => s"Option[$key]" -> returnOption(op)})

  private val extractorsMap: Map[String, Deserializer] = allExtractors withDefaultValue DefaultDeserializer

  val extractors = extractorsMap compose columnTypeNames

  private def opt[X](op: => Any): Option[X] = try {
    Option(op.asInstanceOf[X])
  } catch {
    case ex: Exception => None
  }

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    new H2ORDDIterator(keyName, split.index)
  }

  /** Pass thru an RDD if given one, else pull from the H2O Frame */
  override protected def getPartitions: Array[Partition] = {
    val num = frame.anyVec().nChunks()
    val res = new Array[Partition](num)
    for( i <- 0 until num ) res(i) = new Partition { val index = i }
    res
  }

  class H2ORDDIterator(val keyName: String, val partIndex: Int) extends H2OChunkIterator[A] {

    val columnMapping: Map[Int, Int] = try {
      val mappings = for {
        i <- columnTypeNames.indices
        name = colNames(i)
        j = fr.names().indexOf(name)
      } yield (i, j)

      val bads = mappings collect { case (i, j) if j < 0 => {
        if (i < colNames.length) colNames(i) else s"Unknown index $i (column of type ${columnTypeNames(i)}"
        }
      }
      if (bads.nonEmpty) throw new scala.IllegalArgumentException(s"Missing columns: ${bads mkString ","}")

      mappings.toMap
    }

    private def cell(i: Int) = {
      val j = columnMapping(i)
      val chunk = chks(j)
      val ex = extractors(i)
      val data = ex(chunk, row).asInstanceOf[Object]
      data
    }

    def extractRow: Option[Array[AnyRef]] = opt {
      columnTypeNames.indices map cell toArray
    }

    private var hd: Option[A] = None
    private var total = 0

    override def hasNext = {
      while (hd.isEmpty && super.hasNext) {
        hd = readOne()
        total += 1
      }
      hd.isDefined
    }

    def next(): A = {
      if (hasNext) {
        val a = hd.get
        hd = None
        a
      } else {
        throw new NoSuchElementException(s"No more elements in this iterator: found $total out of $nrows")
      }
    }

    private def readOne(): Option[A] = {
      val dataOpt = extractRow

      row += 1
      val res: Seq[A] = for {
        builder <- builders
        data <- dataOpt
        instance <- builder(data)
      } yield instance

      res.toList match {
        case Nil => None
        case unique :: Nil => Option(unique)
        case one :: two :: more => throw new scala.IllegalArgumentException(
          s"found more than une $jc constructor for given args - can't choose")
      }
    }
  }

  lazy val constructors: Seq[Constructor[_]] = {

    val cs = jc.getConstructors
    val found = cs.collect {
      case c if c.getParameterTypes.length == colNames.length => c
    }

    if (found.isEmpty) throw new scala.IllegalArgumentException(
      s"Constructor must take exactly ${colNames.length} args")

    found
  }

  case class Builder(c:  Constructor[_]) {
    def apply(data: Array[AnyRef]): Option[A] = {
      opt(c.newInstance(data:_*).asInstanceOf[A])
    }
  }

  private lazy val builders = constructors map Builder
}
