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
package ai.h2o.sparkling.backend.api.dataframes

import java.io.File

import ai.h2o.sparkling.backend.exceptions.RestApiCommunicationException
import ai.h2o.sparkling.{H2OFrame, SharedH2OTestContext, TestUtils}
import com.google.gson.JsonParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataFramesServletTestSuite extends FunSuite with SharedH2OTestContext with DataFramesRestApi {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
  import spark.implicits._

  test("DataFrameHandler.list() method") {
    val rdd = sc.parallelize(1 to 10)
    val rid = "df_" + rdd.id
    // create dataframe using method toDF, This is spark method which does not include any metadata
    val df = rdd.toDF("nums")
    df.createOrReplaceTempView(rid)

    val result = listDataFrames()
    assert(result.dataframes.length == 1, "Number of created and persisted DataFramess should be 1")
    assert(result.dataframes(0).dataframe_id == rid, "IDs should match")

    val schema = parseSchema(result.dataframes(0).schema)
    assert(schema.length == df.schema.length, "Number of fields in schemas should be the same")
    assert(
      schema.fields(0).name.equals(df.schema.fields(0).name),
      "Name of the first field in StructType should be the same")
    assert(
      schema.fields(0).nullable == df.schema.fields(0).nullable,
      "Nullable attribute of the first field in StructType should be the same")
    assert(
      schema.fields(0).dataType.typeName.equals(df.schema.fields(0).dataType.typeName),
      "DataType attribute of the first field in StructType should be the same")
    assert(schema.fields(0).metadata == Metadata.empty, "Metadata should be empty")
  }

  test("DataFrameHandler.getDataFrame() method where DataFrame has non empty metadata") {

    val h2oframe = H2OFrame(new File(TestUtils.locate("smalldata/prostate/prostate.csv")))
    // we have created dataFrame from already existing h2oFrame, metadata are included
    val df = hc.asSparkFrame(h2oframe)
    val name = "prostate"
    df.createOrReplaceTempView(name)
    val percentiles = df.schema.fields(0).metadata.getDoubleArray("percentiles")

    val result = getDataFrame(name)
    assert(result.dataframe_id == name, "IDs should match")
    val schema = parseSchema(result.schema)
    assert(schema.length == df.schema.length, "Number of fields in schemas should be the same")
    assert(
      schema.fields(0).name.equals(df.schema.fields(0).name),
      "Name of the first field in StructType should be the same")
    assert(
      schema.fields(0).nullable == df.schema.fields(0).nullable,
      "Nullable attribute of the first field in StructType should be the same")
    assert(
      schema.fields(0).dataType.typeName.equals(df.schema.fields(0).dataType.typeName),
      "DataType attribute of the first field in StructType should be the same")
    assert(
      schema.fields(0).metadata.getDoubleArray("percentiles").sameElements(percentiles),
      "Metadata should match, comparing percentiles")
  }

  test("DataFramesHandler.toH2OFrame() method") {
    val rdd = sc.parallelize(1 to 10)
    val name = "numbers"
    // create dataframe using method toDF, This is spark method which does not include any metadata
    val df = rdd.toDF("nums")
    df.createOrReplaceTempView(name)

    val result = convertToH2OFrame(name, "requested_name")
    val h2oFrame = H2OFrame(result.h2oframe_id)

    assert(h2oFrame.frameId == "requested_name", "H2OFrame ID should be equal to \"requested_name\"")
    assert(h2oFrame.numberOfColumns == df.columns.length, "Number of columns should match")
    assert(h2oFrame.columnNames.sameElements(df.columns), "Column names should match")
    assert(h2oFrame.numberOfRows == df.count(), "Number of rows should match")
  }

  test("DataFramesHandler.getDataFrame() method, querying non existing data frame") {
    intercept[RestApiCommunicationException] {
      getDataFrame("does_not_exist")
    }
  }

  test("DataFramesHandler.toH2OFrame() method, trying to convert dataframe which does not exist") {
    intercept[RestApiCommunicationException] {
      convertToH2OFrame("does_not_exist", null)
    }
  }

  private def parseSchema(schemaString: String): StructType = {
    val parser = new JsonParser
    val obj = parser.parse(schemaString).getAsJsonObject
    val fields = obj.get("fields").getAsJsonArray
    val structFields = new Array[StructField](fields.size())
    for (i <- 0 until fields.size()) {
      val field = fields.get(i).getAsJsonObject
      val name = field.get("name").getAsString
      val nullable = field.get("nullable").getAsBoolean
      val dataType = DataType.fromJson(field.get("type").toString)
      val metadata = Metadata.fromJson(field.get("metadata").toString)
      structFields(i) = StructField(name, dataType, nullable, metadata)
    }
    new StructType(structFields)
  }
}
