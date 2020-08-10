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
package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.params.H2OWord2VecTokenizerParams
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Tokenize Data so they are ready for H2OWord2Vec
  */
class H2OWord2VecTokenizer(override val uid: String)
  extends Transformer
  with H2OWord2VecTokenizerParams
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID(getClass.getSimpleName))

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields ++ Array(StructField(getOutputCol, StringType)))
  }

  private final val MARKER = "empty_line_marker"
  override def transform(dataset: Dataset[_]): DataFrame = {
    require(getInputCol != null, "Input column has to be specified!")
    require(getOutputCol != null, "Output column has to be specified!")
    val withMarker = dataset.withColumn(s"with_marker_$uid", concat(col(getInputCol), lit(" " + MARKER)))
    val tokenizer = new Tokenizer()
      .setInputCol(s"with_marker_$uid")
      .setOutputCol(s"tokenized_$uid")
    val tokenized = tokenizer.transform(withMarker)

    val stopWordsRemover = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setStopWords(getStopWords())
      .setOutputCol(s"stop_words_removed_$uid")

    stopWordsRemover
      .transform(tokenized)
      .withColumn(s"exposed_$uid", explode(col(s"stop_words_removed_$uid")))
      .withColumn(getOutputCol, regexp_replace(col(s"exposed_$uid"), MARKER, ""))
      .drop(s"exposed_$uid", s"stop_words_removed_$uid", s"tokenized_$uid", s"with_marker_$uid")
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object H2OWord2VecTokenizer extends DefaultParamsReadable[H2OWord2VecTokenizer]
