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

import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{Params, StringArrayParam}

trait H2OWord2VecTokenizerParams extends Params with HasInputCol with HasOutputCol {

  //
  // Param definitions
  //
  private final val stopWords = new StringArrayParam(this, "columns", "List of stop words.")
  //
  // Default values
  //
  setDefault(
    stopWords -> Array[String](), // default is empty array which means no columns are removed
    inputCol -> null,
    outputCol -> null
  )

  //
  // Getters
  //
  def getStopWords(): Array[String] = $(stopWords)

  //
  // Setters
  //
  def setStopWords(value: Array[String]): this.type = set(stopWords, value)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}
