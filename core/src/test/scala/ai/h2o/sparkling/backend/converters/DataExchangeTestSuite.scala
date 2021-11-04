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

import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ExposeUtils
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{FunSuite, Matchers}

class DataExchangeTestSuite extends DataFrameConversionSuiteBase {

  private def testConversionFromSparkToH2OAndBack(
      first: ColumnSpecification[_],
      second: ColumnSpecification[_]): Unit = {
    test(s"Convert DataFrame of [${first.field.name}, ${second.field.name}] to H2OFrame and back") {
      val numberOfRows = 20
      val firstValues = first.valueGenerator(numberOfRows)
      val secondValues = second.valueGenerator(numberOfRows)
      val values = firstValues.zip(secondValues).map { case (f, s) => Row(f, s) }
      val rdd = spark.sparkContext.parallelize(values, 4)
      val dataFrame = spark.createDataFrame(rdd, StructType(first.field :: second.field :: Nil))

      val expectedDataFrame = getExpectedDataFrame(dataFrame, numberOfRows)
      val h2oFrame = hc.asH2OFrame(dataFrame)
      val result = hc.asSparkFrame(h2oFrame)

      TestUtils.assertDataFramesAreIdentical(expectedDataFrame, result)
    }
  }

  private def getExpectedDataFrame(original: DataFrame, vectorSize: Int): DataFrame = {
    val newColumns = original.schema.fields.flatMap {
      case StructField(name, dataType, _, _) =>
        val column = col(name)
        dataType match {
          case BooleanType => Seq(column.cast(ByteType))
          case v if ExposeUtils.isMLVectorUDT(v) =>
            val toArr: Any => Array[Double] = (input: Any) => {
              val values = input.asInstanceOf[org.apache.spark.ml.linalg.Vector].toArray
              values ++ Array.fill(vectorSize - values.length)(0.0)
            }
            val toArrUdf = udf(toArr)
            val arrayColumn = toArrUdf(column)
            (0 until vectorSize).map(i => arrayColumn.getItem(i).as(name + i))
          case _ => Seq(column)
        }
    }
    original.select(newColumns: _*)
  }

  allColumns.combinations(2).foreach {
    case Seq(first, second) =>
      testConversionFromSparkToH2OAndBack(first, second)
      testConversionFromSparkToH2OAndBack(second, first)
  }
}
