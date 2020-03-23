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

package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.backend.utils.ConversionUtils.expectedTypesFromClasses
import ai.h2o.sparkling.backend.utils.ReflectionUtils
import ai.h2o.sparkling.backend.utils.SupportedTypes.Double
import ai.h2o.sparkling.backend.{H2OAwareRDD, H2OFrameRelation, Writer, WriterMetadata}
import ai.h2o.sparkling.ml.utils.SchemaUtils._
import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.{H2OContext, H2OFrame, SparkTimeZone}
import org.apache.spark.ExposeUtils
import org.apache.spark.expose.Logging
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, DecimalType}

import scala.util.Random

object SparkDataFrameConverter extends Logging {

  /**
   * Create a Spark DataFrame from a given REST-based H2O frame.
   *
   * @param hc           an instance of H2O context
   * @param fr           an instance of H2O frame
   * @param copyMetadata copy H2O metadata into Spark DataFrame
   * @return a new DataFrame definition using given H2OFrame as data source
   */

  def toDataFrame(hc: H2OContext, fr: ai.h2o.sparkling.H2OFrame, copyMetadata: Boolean): DataFrame = {
    val spark = SparkSessionUtils.active
    val relation = H2OFrameRelation(fr, copyMetadata)(spark.sqlContext)
    spark.baseRelationToDataFrame(relation)
  }

  /** Transform Spark's DataFrame into H2O Frame */
  def toH2OFrame(hc: H2OContext, dataFrame: DataFrame, frameKeyName: Option[String]): H2OFrame = {
    val flatDataFrame = flattenDataFrame(dataFrame)

    val elemMaxSizes = collectMaxElementSizes(flatDataFrame)
    val vecIndices = collectVectorLikeTypes(flatDataFrame.schema).toArray
    val flattenSchema = expandedSchema(flatDataFrame.schema, elemMaxSizes)
    val colNames = flattenSchema.map(_.name).toArray
    val maxVecSizes = vecIndices.map(elemMaxSizes(_))
    val expectedTypes = determineExpectedTypes(flatDataFrame)

    val rdd = flatDataFrame.rdd
    val uniqueFrameId = frameKeyName.getOrElse("frame_rdd_" + rdd.id + randomAlphaNumericString)
    val metadata = WriterMetadata(hc.getConf, uniqueFrameId, expectedTypes, maxVecSizes, SparkTimeZone.current())
    Writer.convert(new H2OAwareRDD(hc.getH2ONodes(), rdd), colNames, metadata)
  }

  private def randomAlphaNumericString: String = Random.alphanumeric.take(20).mkString("")

  private def determineExpectedTypes(flatDataFrame: DataFrame): Array[Byte] = {
    val internalJavaClasses = flatDataFrame.schema.map { f =>
      f.dataType match {
        case n if n.isInstanceOf[DecimalType] & n.getClass.getSuperclass != classOf[DecimalType] => Double.javaClass
        case v if ExposeUtils.isAnyVectorUDT(v) => classOf[Vector]
        case dt: DataType => ReflectionUtils.supportedTypeOf(dt).javaClass
      }
    }.toArray
    expectedTypesFromClasses(internalJavaClasses)
  }
}
