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

package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.backend.utils.SupportedTypes
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

trait HasPlugValues extends H2OAlgoParamsBase {
  private val plugValues = new NullableDictionaryParam[Any](
    this,
    "plugValues",
    "A map containing values that will be used to impute missing values of the training/validation frame, " +
      """use with conjunction missingValuesHandling = "PlugValues")""")

  setDefault(plugValues -> null)

  def getPlugValues(): Map[String, Any] = {
    val values = $(plugValues)
    if (values == null) null else values.asScala.toMap
  }

  def setPlugValues(value: Map[String, Any]): this.type = set(plugValues, if (value == null) null else value.asJava)

  private def getPlugValuesFrameKey(): String = {
    val plugValues = getPlugValues()
    if (plugValues == null) {
      null
    } else {
      val spark = SparkSessionUtils.active
      val row = new GenericRow(plugValues.values.toArray)
      val rows = Seq[Row](row).asJava
      val fields = plugValues.map{ case (key, value) =>
        val sparkType = SupportedTypes.simpleByName(value.getClass.getSimpleName).sparkType
        StructField(key, sparkType, nullable = false)
      }.toArray
      val schema = StructType(fields)
      val df = spark.createDataFrame(rows, schema)
      val hc = H2OContext.ensure(
        s"H2OContext needs to be created in order to train the ${this.getClass.getSimpleName} model. " +
          "Please create one as H2OContext.getOrCreate().")
      val frame = hc.asH2OFrame(df)
      val stringFieldsIndices = fields.zipWithIndex.filter(_._1.dataType == StringType).map(_._2)
      if (stringFieldsIndices.nonEmpty) {
        frame.convertColumnsToCategorical(stringFieldsIndices)
      }
      frame.frameId
    }
  }

  override private[sparkling] def getH2OAlgorithmParams(): Map[String, Any] = {
    super.getH2OAlgorithmParams() ++ Map("plug_values" -> getPlugValuesFrameKey())
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("plugValues" -> "plug_values")
  }
}
