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
package water.api

import java.io.File

import com.google.gson.JsonParser
import org.apache.spark.SparkContext
import org.apache.spark.h2o.util.SharedSparkTestContext
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.DKV
import water.api.DataFrames._
import water.exceptions.H2ONotFoundArgumentException
import water.fvec.{Frame, H2OFrame}

/**
 * Test suite for DataFrames handler
 */
@RunWith(classOf[JUnitRunner])
class DataFramesHandlerSuite extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)
  test("DataFrameHandler.list() method") {
    val rdd = sc.parallelize(1 to 10)
    val rid = "df_" + rdd.id
    val sqlContext = sqlc
    import sqlContext.implicits._
    // create dataframe using method toDF, This is spark method which does not include any metadata
    val df = rdd.toDF("nums")

    df.registerTempTable(rid)
    val dataFramesHandler = new DataFramesHandler(sc,hc)

    val req = new DataFramesV3
    val result = dataFramesHandler.list(3, req)
    assert (result.dataframes.length == 1, "Number of created and persisted DataFramess should be 1")
    assert (result.dataframes(0).dataframe_id == rid, "IDs should match")

    val schema = parseSchema(result.dataframes(0).schema)
    assert (schema.length == df.schema.length,
            "Number of fields in schemas should be the same")
    assert (schema.fields(0).name.equals(df.schema.fields(0).name),
            "Name of the first field in StructType should be the same")
    assert (schema.fields(0).nullable == df.schema.fields(0).nullable,
            "Nullable attribute of the first field in StructType should be the same")
    assert (schema.fields(0).dataType.typeName.equals(df.schema.fields(0).dataType.typeName),
            "DataType attribute of the first field in StructType should be the same")
    assert (schema.fields(0).metadata == Metadata.empty,
            "Metadata should be empty")
  }

  test("DataFrameHandler.getDataFrame() method where DataFrame has non empty metadata") {

    val h2oframe = new H2OFrame(new File("./examples/smalldata/prostate.csv"))
    // we have created dataFrame from already existing h2oFrame, metadata are included
    val df = hc.asDataFrame(h2oframe)
    val name= "prostate"
    df.registerTempTable(name)
    val percentiles = df.schema.fields(0).metadata.getDoubleArray("percentiles")
    val dataFramesHandler = new DataFramesHandler(sc,hc)

    val req = new DataFrameV3
    req.dataframe_id = name
    val result = dataFramesHandler.getDataFrame(3, req)

    assert (result.dataframe_id == name, "IDs should match")
    val schema = parseSchema(result.schema)
    assert (schema.length == df.schema.length,
            "Number of fields in schemas should be the same")
    assert (schema.fields(0).name.equals(df.schema.fields(0).name),
            "Name of the first field in StructType should be the same")
    assert (schema.fields(0).nullable == df.schema.fields(0).nullable,
            "Nullable attribute of the first field in StructType should be the same")
    assert (schema.fields(0).dataType.typeName.equals(df.schema.fields(0).dataType.typeName),
            "DataType attribute of the first field in StructType should be the same")
    assert (schema.fields(0).metadata.getDoubleArray("percentiles").sameElements(percentiles),
            "Metadata should match, comparing percentiles")
  }

  test("DataFramesHandler.toH2OFrame() method"){
    val sqlContext = sqlc
    import sqlContext.implicits._
    val rdd = sc.parallelize(1 to 10)
    val name = "numbers"
    // create dataframe using method toDF, This is spark method which does not include any metadata
    val df = rdd.toDF("nums")
    df.registerTempTable(name)
    val dataFramesHandler = new DataFramesHandler(sc,hc)

    val req = new H2OFrameIDV3
    req.dataframe_id = name
    req.h2oframe_id ="requested_name"
    val result = dataFramesHandler.toH2OFrame(3, req)

    // create h2o frame for the given id
    val value = DKV.get(result.h2oframe_id)
    val h2oFrame: H2OFrame = value.className() match {
      case name if name.equals(classOf[Frame].getName) => {
        val h2oContext = hc
        import h2oContext.implicits._
        value.get[Frame]()
      }
      case name if name.equals(classOf[H2OFrame].getName) => value.get[H2OFrame]()
    }
    assert (h2oFrame.key.toString == "requested_name", "H2OFrame ID should be equal to \"requested_name\"")
    assert (h2oFrame.numCols()==df.columns.size, "Number of columns should match")
    assert (h2oFrame.names().sameElements(df.columns),"Column names should match")
    assert (h2oFrame.numRows() == df.count(), "Number of rows should match")
  }


  test("DataFramesHandler.getDataFrame() method, querying non existing data frame"){
    val dataFramesHandler = new DataFramesHandler(sc,hc)
    val req = new DataFrameV3
    req.dataframe_id = "does_not_exist"
    intercept[H2ONotFoundArgumentException] {
      dataFramesHandler.getDataFrame(3, req)
    }
  }

  test("DataFramesHandler.toH2OFrame() method, trying to convert dataframe which does not exist"){
    val dataFramesHandler = new DataFramesHandler(sc,hc)
    val req = new H2OFrameIDV3
    req.dataframe_id = "does_not_exist"

    intercept[H2ONotFoundArgumentException] {
      dataFramesHandler.toH2OFrame(3, req)
    }
  }

  def parseSchema(schemaString: String) : StructType = {
    import com.google.gson.Gson
    val gson = new Gson()
    val parser = new JsonParser
    val obj = parser.parse(schemaString).getAsJsonObject
    val fields = obj.get("fields").getAsJsonArray
    val structFields = new Array[StructField](fields.size())
    for( i <- 0 until fields.size()){
      val field = fields.get(i).getAsJsonObject
      val name = field.get("name").getAsString
      val nullable = field.get("nullable").getAsBoolean
      val dataType = DataType.fromJson(field.get("type").toString)
      val metadata = Metadata.fromJson(field.get("metadata").toString)
      structFields(i) = new StructField(name,dataType,nullable,metadata)
    }
    new StructType(structFields)
  }

}
