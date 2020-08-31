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

import org.apache.spark.ml.param.{IntParam, Param, Params, StringArrayParam}

trait H2OWord2VecTokenizerParams extends Params {

  //
  // Param definitions
  //
  private final val stopWords = new StringArrayParam(this, "stopWords", "List of stop words.")
  private final val inputCol = new Param[String](this, "inputCol", "input column name")
  private final val outputCol = new Param[String](this, "outputCol", "output column name")
  private final val pattern = new Param[String](this, "pattern", "Regex pattern")
  private final val minTokenLength = new IntParam(this, "minTokenLength", "Minimal length of a word")
  //
  // Default values
  //
  setDefault(stopWords -> Array.empty, outputCol -> (uid + "__output"), pattern -> "\\W", minTokenLength -> 2)

  //
  // Getters
  //
  def getStopWords(): Array[String] = $(stopWords)

  def getInputCol(): String = $(inputCol)

  def getOutputCol(): String = $(outputCol)

  def getPattern(): String = $(pattern)

  def getMinTokenLength(): Int = $(minTokenLength)

  //
  // Setters
  //
  def setStopWords(value: Array[String]): this.type = set(stopWords, value)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setPattern(value: String): this.type = set(pattern, value)

  def setMinTokenLength(value: Int): this.type = set(minTokenLength, value)
}
