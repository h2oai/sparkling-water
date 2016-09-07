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


import java.lang.reflect.Constructor
import language.postfixOps
import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import water.fvec.Frame

import scala.collection.immutable.IndexedSeq
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Convert H2OFrame into an RDD (lazily).
  *
  * @param frame  an instance of H2O frame
  * @param colNamesInResult names of columns
  * @param sc  an instance of Spark context
  * @tparam A  type for resulting RDD
  * @tparam T  specific type of H2O frame
  */
private[spark]
class H2ORDD[A <: Product: TypeTag: ClassTag, T <: Frame] private(@transient val frame: T,
                                                                  val colNamesInResult: Array[String])
                                                                 (@transient sc: SparkContext)
  extends {
    override val isExternalBackend = H2OConf(sc).runsInExternalClusterMode
  } with RDD[A](sc, Nil) with H2ORDDLike[T] {

  // Get column names before building an RDD
  def this(@transient fr : T)
          (@transient sc: SparkContext) = this(fr, ReflectionUtils.names[A])(sc)

  // Check that H2OFrame & given Scala type are compatible
  if (colNamesInResult.length > 1) {
    colNamesInResult.foreach { name =>
      if (frame.find(name) == -1) {
        throw new IllegalArgumentException("Scala type has field " + name +
          " but H2OFrame does not have a matching column; has " + frame.names().mkString(","))
      }
    }
  }

  /** Number of columns in the full dataset */
  val numColsInFrame = frame.numCols()

  val colNamesInFrame = frame.names()

  val types = ReflectionUtils.types[A](colNamesInResult)
  val expectedTypesAll: Option[Array[Byte]] = ConverterUtils.prepareExpectedTypes(isExternalBackend, types)

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    val iterator = new H2ORDDIterator(frameKeyName, split.index)
    ConverterUtils.getIterator[A](isExternalBackend, iterator)
  }

  private val columnTypeNames = ReflectionUtils.typeNames[A](colNamesInResult)

  private val jc = implicitly[ClassTag[A]].runtimeClass

  private type OptionReader = Int => Option[Any]
  private type Reader = Int => Any

  private val DefaultReader: Reader = _ => None

  private type TypeName = String

  /**
    * Given a ReadConverterContext and a column number, returns an Option[T]
    * with the value parsed according to TypeName.
    * You can override it.
    *
    * @param source read converter
    * @return a map, from type name to option reader
    */
  def optionReaders(source: ReadConverterContext): Map[TypeName, OptionReader] = Map(
    "Boolean" -> ((col: Int) => source.getBoolean(col)),
    "Byte"    -> ((col: Int) => source.getByte(col)),
    "Double"  -> ((col: Int) => source.getDouble(col)),
    "Float"   -> ((col: Int) => source.getFloat(col)),
    "Integer" -> ((col: Int) => source.getInt(col)),
    "Long"    -> ((col: Int) => source.getLong(col)),
    "Short"   -> ((col: Int) => source.getShort(col)),
    "String"  -> ((col: Int) => source.getString(col)))

  /**
    * Given a type name, returns a substitute value for the case it's NaN in spark
    * You can override it.
    */
  val NaNs: Map[TypeName, Any] = Map(
    "Boolean" -> false,
    "Byte"    -> 0.toByte,
    "Double"  -> Double.NaN,
    "Float"   -> Float.NaN,
    "Integer" -> 0,
    "Long"    -> 0L,
    "Short"   -> 0.toShort,
    "String"  -> null
  )

  private def twoReaders = (key: TypeName, op: OptionReader) =>
    List((s"Option[$key]":TypeName) -> op,
         key                        -> ((col: Int) => op(col).getOrElse(NaNs(key)))
    )

  private def availableReaders(source: ReadConverterContext): Map[TypeName, Reader] =
    optionReaders(source) flatMap twoReaders.tupled

  private def readerMap(source: ReadConverterContext): Map[TypeName, Reader] = availableReaders(source: ReadConverterContext) withDefaultValue DefaultReader

  private def columnReaders(source: ReadConverterContext) = readerMap(source) compose columnTypeNames

  private def opt[X](op: => Any): Option[X] = try {
    Option(op.asInstanceOf[X])
  } catch {
    case ex: Exception => None
  }
  // maps data columns to product components
  val columnMapping: Map[Int, Int] =
    if (columnTypeNames.size == 1) Map(0->0) else multicolumnMapping

  def multicolumnMapping: Map[Int, Int] = {
    try {
      val mappings = for {
        i <- columnTypeNames.indices
        name = colNamesInResult(i)
        j = colNamesInFrame.indexOf(name)
      } yield (i, j)

      val bads = mappings collect {
        case (i, j) if j < 0 =>
          if (i < colNamesInResult.length) colNamesInResult(i) else s"[[$i]] (column of type ${columnTypeNames(i)}"
      }

      if (bads.nonEmpty) {
        throw new scala.IllegalArgumentException(s"Missing columns: ${bads mkString ","}")
      }

      mappings.toMap
    }
  }

  class H2ORDDIterator(val keyName: String, val partIndex: Int) extends H2OChunkIterator[A] {

    override lazy val converterCtx: ReadConverterContext =
      ConverterUtils.getReadConverterContext(keyName,
        partIndex)

    private lazy val readers = columnReaders(converterCtx)

    private def convertPerColumn(i: Int) = {
      val j = columnMapping(i)
      val ex = readers(i)
      val data = ex(j).asInstanceOf[Object]
      data
    }

    def extractRow: Option[Array[AnyRef]] = {
      val rowOpt = opt {
        val objects: IndexedSeq[Object] = columnTypeNames.indices map convertPerColumn
        val row = objects toArray

        row
      }
      converterCtx.increaseRowIdx()
      rowOpt
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
        throw new NoSuchElementException(s"No more elements in this iterator: found $total  out of ${converterCtx.numRows}")
            }
      }

    /**
      * This function takes a row of raw data (array of Objects) and transforms it
      * to a value of type A (if possible).
      * extractRow gives us the array (if succeeds)
      * On this array it tries to apply a builder that accepts that data.
      * A Builder has a constructor inside.
      * If exactly one attempt to build an instance succeeds, we are good.
      * If no attempts succeeded, we return a None.
      * If more than one constructor produced an instance, it means we are out of luck,
      * there are too many constructors for this kind of data; and an exception is thrown.
      *
      * @return Option[A], that is Some(result:A), if succeeded, or else None
      */
      private def readOne(): Option[A] = {
            val dataOpt = extractRow

            val res: Seq[A] = for {
            builder <- builders
            data <- dataOpt
            instance <- builder(data)
            } yield instance

            res.toList match {
              case Nil => None
              case unique :: Nil => Option(unique)
              case one :: two :: more =>
                throw new scala.IllegalArgumentException(
                                         s"found more than one $jc constructor for given args - can't choose")
          }
        }

  }


  lazy val constructors: Seq[Constructor[_]] = {

    val cs = jc.getConstructors
    val found = cs.collect {
      case c if c.getParameterTypes.length == colNamesInResult.length => c
    }

    if (found.isEmpty) throw new scala.IllegalArgumentException(
      s"Constructor must take exactly ${colNamesInResult.length} args")

    found
  }

  case class Builder(c:  Constructor[_]) {
    def apply(data: Array[AnyRef]): Option[A] = {
      opt(c.newInstance(data:_*).asInstanceOf[A])
    }
  }

  private lazy val builders = constructors map Builder

}
