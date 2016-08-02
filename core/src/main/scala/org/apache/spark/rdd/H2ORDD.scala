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


import java.lang
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
  val types = ReflectionUtils.types[A](colNames)
  val jc = implicitly[ClassManifest[A]].runtimeClass

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

  def opt[T](op: => Any): Option[T] = try {
    Option(op.asInstanceOf[T])
  } catch {
    case x: Exception => None
  }

  class H2ORDDIterator(val keyName: String, val partIndex: Int) extends H2OChunkIterator[A] {
    /** Dummy muttable holder for String values */
    val valStr = new BufferedString()

    val columnMapping: Map[Int, Int] = try {
      val mappings = for {
        i <- types.indices
        name = colNames(i)
        j = fr.names().indexOf(name)
      } yield (i, j)

      val bads = mappings collect { case (i, j) if j < 0 => colNames(j) }
      if (bads.nonEmpty) throw new scala.IllegalArgumentException(s"Missing columns: ${bads mkString ","}")

      mappings.toMap
    }

    def extractOptions: Array[Option[Any]] = {
      val data = new Array[Option[Any]](types.length)
      for {
        i <- types.indices
        j = columnMapping(i)
        chk = chks(j)
        typ = types(i)
      } {
        data(i) = valueOf(chk, typ)
      }
      data
    }

    def valueOf(chk: Chunk, typ: Class[_]): Option[Nothing] = {
      if (chk.isNA(row)) None
      else typ match {
        case q if q == classOf[Integer] => opt(chk.at8(row).asInstanceOf[Int])
        case q if q == classOf[lang.Long] => opt(chk.at8(row))
        case q if q == classOf[lang.Double] => opt(chk.atd(row))
        case q if q == classOf[lang.Float] => opt(chk.atd(row))
        case q if q == classOf[lang.Boolean] => opt(chk.at8(row) == 1)
        case q if q == classOf[String] =>
          if (chk.vec().isCategorical) {
            opt(chk.vec().domain()(chk.at8(row).asInstanceOf[Int]))
          } else if (chk.vec().isString) {
            chk.atStr(valStr, row)
            opt(valStr.toString)
          } else None
        case _ => None
      }
    }

    private var hd: Option[A] = None
    private var total = 0

    override def hasNext = {
      while (!hd.isDefined && super.hasNext) {
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
      } else
        throw new NoSuchElementException(s"No more elements in this iterator: found $total out of $nrows")
    }

    private def readOne(): Option[A] = {
      val options: Array[Option[Any]] = extractOptions
      val values: List[Array[Object]] = try {
        val extractedValues: Array[Object] = for {
          opt <- options
          v: AnyRef = opt.get.asInstanceOf[AnyRef]
        } yield v

        extractedValues :: Nil
      } catch {
        case oops: Exception => Nil
      }
      val optionValues: Array[AnyRef] = options.map(x => x)
      val allDataArrays: List[Array[AnyRef]] = optionValues :: values

      row += 1
      val res: Seq[A] = for {
        builder <- builders
        data <- allDataArrays
        instance <- builder(data)
      } yield instance

      res.toList match {
        case Nil =>
          None
        //throw new scala.IllegalArgumentException(
        //s"Failed to find an appropriate $jc constructor for given args")
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
