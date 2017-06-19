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

package org.apache.spark.ml.h2o.features

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{AnyType, AnyTypeUDT, DataFrame, Dataset}

/**
  * This transformer creates a single vector of features from given columns.
  * This vector, contrary to MLLib feature vectors, does not have to contain only doubles,
  * other values such as string are also properly handled.
  */
class ColumnVectorizer(override val uid: String) extends Transformer with ColumnConcatParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("h2oColumnVectorizer"))

  /**
    * Set the keep value which decides whether we should only keep the given columns or remove them.
    *
    * @param value
    * @return
    * @group setParam
    */
  def setKeep(value: Boolean): this.type = set(keep, value)

  /**
    * Columns that should be either kept or removed depending on the keep value.
    *
    * @param value
    * @return
    * @group setParam
    */
  def setColumns(value: Array[String]): this.type = set(columns, value)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.appendColumn(schema, $(outputCol), new ArrayType(new AnyTypeUDT(), false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val columnsToVectorize = if($(keep)){
      dataset.columns.filter(!$(columns).contains(_)).map(dataset.col)
    }else{
      dataset.columns.filter($(columns).contains(_)).map(dataset.col)
    }

    val outputSchema = transformSchema(dataset.schema)
    val t = udf { terms: Seq[_] => terms.map {
      case i: Int => new AnyType(i.toString, "int")
      case s: String => new AnyType(s, "string")
      // TODO implement
      case _ => new AnyType("", "")
    }}
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(columnsToVectorize:_*).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object ColumnVectorizer extends DefaultParamsReadable[ColumnVectorizer]

trait ColumnConcatParams extends Params with HasOutputCol {
  /**
    * By default it is set to false which means removing specified columns
    */
  final val keep = new BooleanParam(this, "keep", "Determines if the column specified in the 'columns' parameter should be kept or removed")

  setDefault(keep->false)

  /** @group getParam */
  def getKeep: Boolean = $(keep)

  /**
    * By default it is empty array which means no columns are removed
    */
  final val columns = new StringArrayParam(this, "columns", "List of columns to be kept or removed")

  setDefault(columns->Array[String]())

  /** @group getParam */
  def getColumns: Array[String] = $(columns)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)
}
