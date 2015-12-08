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
import java.util

import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.SparkContext
import org.apache.spark.h2o._
import org.apache.spark.h2o.util.{SparkTestContext, SharedSparkTestContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, DataType, Metadata, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.DKV
import water.api.DataFrames._
import water.api.RDDs.{RDDsV3, RDDsHandler}
import water.fvec.{Frame, H2OFrame}

import scala.util.parsing.json.{JSONObject, JSON}

/**
 * Test suite for dataframes end-points
 */
@RunWith(classOf[JUnitRunner])
class DataFramesHandlerSuite extends FunSuite with SparkTestContext {
  sc = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)
  hc = H2OContext.getOrCreate(sc)
  // Shared h2oContext
  val h2oContext = hc
  // Shared sqlContext
  implicit val sqlContext = new SQLContext(sc)

  test("DataFrameHandler.list() method") {
    import sqlContext.implicits._

    val rdd = sc.parallelize(1 to 10)
    val rid = "df_" + rdd.id

    // create dataframe using method toDF, This is spark method which does not include any metadata
    val df = rdd.toDF("nums")
    df.registerTempTable(rid)
    val dataFramesHandler = new DataFramesHandler(sc,h2oContext)

    val req = new DataFramesV3
    val result = dataFramesHandler.list(3, req)
    assert (result.dataFrames.length == 1, "Number of created and persisted DataFramess should be 1")
    assert (result.dataFrames(0).dataframe_id == rid, "IDs should match")

    val schema = parseSchema(result.dataFrames(0).schema)
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
    val df = h2oContext.asDataFrame(h2oframe)
    val name= "prostate"
    df.registerTempTable(name)
    val percentiles = df.schema.fields(0).metadata.getDoubleArray("percentiles")
    val dataFramesHandler = new DataFramesHandler(sc,h2oContext)

    val req = new DataFrameWithMsgV3
    req.searched_dataframe_id = name
    val result = dataFramesHandler.getDataFrame(3, req)

    assert (result.dataframe.dataframe_id == name, "IDs should match")
    assert (result.msg.equals("OK"),"Status should be OK")
    val schema = parseSchema(result.dataframe.schema)
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
    import sqlContext.implicits._

    val rdd = sc.parallelize(1 to 10)
    val name = "numbers"
    // create dataframe using method toDF, This is spark method which does not include any metadata
    val df = rdd.toDF("nums")
    df.registerTempTable(name)
    val dataFramesHandler = new DataFramesHandler(sc,h2oContext)

    val req = new H2OFrameIDV3
    req.dataframe_id = name
    val result = dataFramesHandler.toH2OFrame(3, req)

    // create h2o frame for the given id
    val value = DKV.get(result.h2oframe_id)
    val h2oFrame: H2OFrame = value.className() match {
      case name if name.equals(classOf[Frame].getName) => {
        h2oContext.asH2OFrame(value.get[Frame]())
      }
      case name if name.equals(classOf[H2OFrame].getName) => value.get[H2OFrame]()
    }
    assert (result.msg.equals("Success"),"Status should be Success")
    assert (h2oFrame.numCols()==df.columns.size, "Number of columns should match")
    assert (h2oFrame.names().sameElements(df.columns),"Column names should match")
    assert (h2oFrame.numRows() == df.count(), "Number of rows should match")
  }


  test("DataFramesHandler.getDataFrame() method, querying non existing data frame"){
    val dataFramesHandler = new DataFramesHandler(sc,h2oContext)
    val req = new DataFrameWithMsgV3
    req.searched_dataframe_id = "does_not_exist"
    val result = dataFramesHandler.getDataFrame(3, req)
    assert (result.dataframe == null, "Returned dataframe should be null")
    assert (!result.msg.equals("OK"), "Status is not OK - it is message saying that given dataframe does not exist")
  }

  test("DataFramesHandler.toH2OFrame() method, trying to convert dataframe which does not exist"){
    val dataFramesHandler = new DataFramesHandler(sc,h2oContext)
    val req = new H2OFrameIDV3
    req.dataframe_id = "does_not_exist"
    val result = dataFramesHandler.toH2OFrame(3, req)
    assert (result.h2oframe_id.equals(""), "Returned H2O Frame id should be empty")
    assert (!result.msg.equals("OK"), "Status is not OK - it is message saying that given dataframe does not exist")
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
    return new StructType(structFields)
  }
}
