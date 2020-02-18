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

package ai.h2o.sparkling.backend.shared

import java.lang.reflect.Constructor

import org.apache.spark.h2o.utils.ProductType

import scala.annotation.meta.{field, getter}
import scala.language.postfixOps
import scala.reflect.ClassTag


/**
 * The abstract class contains common methods for client-based and REST-based RDDs.
 */
private[backend] trait H2ORDDBase[A <: Product] extends H2OSparkEntity {

  def productType: ProductType

  protected def colNames: Array[String]

  /**
   * The method checks that H2OFrame & given Scala type are compatible
   */
  protected def checkColumnNames(columnNames: Array[String]): Unit = {
    if (!productType.isSingleton) {
      val productFields = productType.members.map(_.name)
      val problems = productFields.diff(columnNames).mkString(", ")

      if (problems.nonEmpty) {
        throw new IllegalArgumentException(s"The following fields are missing in frame: $problems; " +
          "we have " + columnNames.mkString(","))
      }
    }
  }
  protected val jc : Class[_]

  private def columnReaders(rcc: Reader) = productType.memberTypeNames map rcc.readerMapByName

  private def opt[X](op: => Any): Option[X] = try {
    Option(op.asInstanceOf[X])
  } catch {
    case _: Exception =>
      None
  }

  // maps data columns to product components
  lazy val columnMapping: Array[Int] = if (productType.isSingleton) Array(0) else multicolumnMapping

  def multicolumnMapping: Array[Int] = {
    val mappings: Array[Int] = productType.members map (colNames indexOf _.name)

    val bads = mappings.zipWithIndex collect {
      case (j, at) if j < 0 =>
        if (at < productType.arity) productType.members(at).toString else s"[[$at]] (unknown type)"
    }

    if (bads.nonEmpty) {
      throw new scala.IllegalArgumentException(s"Missing columns: ${bads mkString ","}")
    }

    mappings
  }

  override lazy val selectedColumnIndices: Array[Int] = columnMapping

  abstract class H2ORDDIterator extends H2OChunkIterator[A] {

    private lazy val readers = columnReaders(reader)

    private lazy val convertPerColumn: Array[() => AnyRef] =
      (columnMapping zip readers) map { case (j, r) => () =>
        r.apply(j).asInstanceOf[AnyRef] // this last trick converts primitives to java.lang wrappers
      }

    def extractRow: Array[AnyRef] = {
      val rowOpt = convertPerColumn map (_ ())
      reader.increaseRowIdx()
      rowOpt
    }

    private var cachedRow: Option[A] = None
    private var rowsRead = 0

    /**
     * Checks if there is a next value in the stream.
     * Note that reading does not always lead to a success,
     * so there are the following options:
     * - the previous read succeeded, and we have a cached value stored
     * then we are fine and no further actions are required
     * - there is no cached value, and the underlying iterator says it has more data.
     * In this case we try to read the row. This may succeed or not
     * - if it succeeds, we are good
     * - if it does not succeed, it probably means the value is no good (e.g. an exception thrown when
     * we try to read a string column as a Double, etc;
     * in this case we repeat the attempt - until either we receive a good value, or we are out of rows
     *
     */
    override def hasNext = {
      while (cachedRow.isEmpty && super.hasNext) {
        cachedRow = readOneRow()
        rowsRead += 1
      }
      cachedRow.isDefined
    }

    def next(): A = {
      if (hasNext) {
        val row = cachedRow.get
        cachedRow = None
        row
      } else {
        throw new NoSuchElementException(s"No more elements in this iterator: found $rowsRead  out of ${reader.numRows}")
      }
    }

    /**
     * This function reads a row of raw data (array of Objects) and transforms
     * it to a value of type A (if possible).
     *
     * extractRow function gives us the array or raw data (if succeeds)
     * It tries to apply an instance builder on this array, where
     * an instance builder tries to create new instance based on the given data.
     *
     * If exactly one attempt to build an instance succeeds, we are good.
     * If no attempts succeeded, we return a None.
     * If more than one constructor ca produce an instance, it means we are out of luck,
     * there are too many constructors for this kind of data; and an exception is thrown.
     *
     * @return Option[A], that is Some(result:A), if succeeded, or else None
     */
    private def readOneRow(): Option[A] = {
      val data = extractRow

      val res: Seq[A] = for {
        builder <- instanceBuilders
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
      case c if c.getParameterTypes.length == productType.arity => c
    }

    if (found.isEmpty) throw new scala.IllegalArgumentException(
      s"Constructor must take exactly ${productType.arity} args")

    found
  }

  @transient private case class InstanceBuilder(c: Constructor[_]) {
    def apply(data: Array[AnyRef]): Option[A] = opt(c.newInstance(data: _*).asInstanceOf[A])
  }

  @(transient@field @getter) private lazy val instanceBuilders = constructors map InstanceBuilder
}
