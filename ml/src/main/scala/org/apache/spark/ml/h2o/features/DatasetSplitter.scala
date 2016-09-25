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

import hex.FrameSplitter
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.h2o.OneTimeTransformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import water.fvec.{Frame, H2OFrame}
import water.{DKV, Key}

/**
  *  Split the dataset and store the splits with the specified keys into DKV
  *  It determines the frame which is passed on the output in the following order:
  *     1) If the train key is specified using setTrainKey method and the key is also specified in the list of keys,
  *        then frame with this key is passed on the output
  *     2) Otherwise, if the default key - "train.hex" is specified in the list of keys, then frame with this key
  *        is passed on the output
  *     3) Otherwise the first frame specified in the list of keys is passed on the output
*/
class DatasetSplitter(override val uid: String)
                     (implicit val h2oContext: H2OContext, sqlContext: SQLContext)
  extends OneTimeTransformer with DatasetSplitterParams with DefaultParamsWritable{

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Identifiable.randomUID("h2oFrameSplitter"))

  private def split(df: H2OFrame, keys: Seq[String], ratios: Seq[Double]): Array[Frame] = {
    val ks = keys.map(Key.make[Frame]).toArray
    val splitter = new FrameSplitter(df, ratios.toArray, ks, null)
    water.H2O.submitTask(splitter)
    // return results
    splitter.getResult
  }

  /** @group setParam */
  def setRatios(value: Array[Double]) = set(ratios, value)

  /** @group setParam */
  def setKeys(value: Array[String]) = set(keys, value)

  /** @group setParam
    *
    * When specified trainKey is not in the list of keys, the splitter passes the first split as train dataset
    * */
  def setTrainKey(value: String) = set(trainKey, value)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    require($(keys).nonEmpty, "Keys can not be empty")

    import h2oContext.implicits._
    split(dataset.toDF(),$(keys),$(ratios))

    val returnKey = if($(keys).contains($(trainKey))){
      $(trainKey)
    }else{
      $(keys)(0) // first key
    }

    h2oContext.asDataFrame(DKV.getGet[Frame](returnKey))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object DatasetSplitter extends MLReadable[DatasetSplitter]{

  private class DatasetSplitterReader extends MLReader[DatasetSplitter] {

    private val className = classOf[DatasetSplitter].getName

    override def load(path: String): DatasetSplitter = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val h2oContext = H2OContext.ensure("H2OContext has to be started in order to use H2O pipelines elements")
      val datasetSplitter = new DatasetSplitter(metadata.uid)(h2oContext, sqlContext)
      DefaultParamsReader.getAndSetParams(datasetSplitter, metadata)
      datasetSplitter
    }
  }

  @Since("1.6.0")
  override def read: MLReader[DatasetSplitter] = new DatasetSplitterReader

  @Since("1.6.0")
  override def load(path: String): DatasetSplitter = super.load(path)
}

trait DatasetSplitterParams extends Params {

  /**
    * By default it is set to Array(1.0) which does not split the dataset at all
    */
  final val ratios = new DoubleArrayParam(this, "ratios", "Determines in which ratios split the dataset")

  setDefault(ratios->Array[Double](1.0))

  /** @group getParam */
  def getRatios: Array[Double] = $(ratios)

  /**
    * By default it is set to Array("train.hex")
    */
  final val keys = new StringArrayParam(this, "keys", "Sets the keys for split frames")

  setDefault(keys->Array[String]("train.hex"))

  /** @group getParam */
  def getKeys: Array[String] = $(keys)

  /**
    * By default it is set to "train.hex"
    */
  final val trainKey = new Param[String](this, "trainKey", "Specify which key from keys specify the training dataset")

  setDefault(trainKey->"train.hex")

  /** @group getParam */
  def getTrainKey: String = $(trainKey)

}
