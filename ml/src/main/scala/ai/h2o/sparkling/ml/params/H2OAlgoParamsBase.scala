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

import ai.h2o.sparkling.{H2OContext, H2OFrame}
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.ml.param._
import org.apache.spark.sql.SparkSession

trait H2OAlgoParamsBase extends Params {
  private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = Map.empty

  private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = Map.empty

  protected def booleanParam(name: String, doc: String): BooleanParam = {
    new BooleanParam(this, name, doc)
  }

  protected def intParam(name: String, doc: String): IntParam = {
    new IntParam(this, name, doc)
  }

  protected def longParam(name: String, doc: String): LongParam = {
    new LongParam(this, name, doc)
  }

  protected def floatParam(name: String, doc: String): FloatParam = {
    new FloatParam(this, name, doc)
  }

  protected def doubleParam(name: String, doc: String): DoubleParam = {
    new DoubleParam(this, name, doc)
  }

  protected def param[T](name: String, doc: String): Param[T] = {
    new Param[T](this, name, doc)
  }

  protected def stringParam(name: String, doc: String): Param[String] = {
    new Param[String](this, name, doc)
  }

  protected def nullableStringParam(name: String, doc: String): NullableStringParam = {
    new NullableStringParam(this, name, doc)
  }

  protected def stringArrayParam(name: String, doc: String): StringArrayParam = {
    new StringArrayParam(this, name, doc)
  }

  protected def intArrayParam(name: String, doc: String): IntArrayParam = {
    new IntArrayParam(this, name, doc)
  }

  protected def doubleArrayParam(name: String, doc: String): DoubleArrayParam = {
    new DoubleArrayParam(this, name, doc)
  }

  protected def nullableDoubleArrayArrayParam(name: String, doc: String): NullableDoubleArrayArrayParam = {
    new NullableDoubleArrayArrayParam(this, name, doc)
  }

  protected def nullableIntArrayParam(name: String, doc: String): NullableIntArrayParam = {
    new NullableIntArrayParam(this, name, doc)
  }

  protected def nullableFloatArrayParam(name: String, doc: String): NullableFloatArrayParam = {
    new NullableFloatArrayParam(this, name, doc)
  }

  protected def nullableDoubleArrayParam(name: String, doc: String): NullableDoubleArrayParam = {
    new NullableDoubleArrayParam(this, name, doc)
  }

  protected def nullableStringArrayParam(name: String, doc: String): NullableStringArrayParam = {
    new NullableStringArrayParam(this, name, doc)
  }

  private def convertWithH2OContext[TInput <: AnyRef, TOutput <: AnyRef](input: TInput)(
      body: (SparkSession, H2OContext) => TOutput): TOutput = {
    if (input == null) {
      null.asInstanceOf[TOutput]
    } else {
      val spark = SparkSessionUtils.active
      val hc = H2OContext.ensure(
        s"H2OContext needs to be created in order to train the ${this.getClass.getSimpleName} model. " +
          "Please create one as H2OContext.getOrCreate().")
      body(spark, hc)
    }
  }

  protected def convert2dArrayToH2OFrame(input: Array[Array[Double]]): String = {
    convertWithH2OContext(input) { (spark, hc) =>
      import spark.implicits._
      val df = spark.sparkContext.parallelize(input).toDF()
      hc.asH2OFrame(df).frameId
    }
  }

  protected def convertVectorArrayToH2OFrameKeyArray(vectors: Array[DenseVector]): Array[String] = {
    convertWithH2OContext(vectors) { (spark, hc) =>
      import spark.implicits._
      vectors.map { vector =>
        val df = spark.sparkContext.parallelize(vector.values).toDF()
        hc.asH2OFrame(df).frameId
      }
    }
  }

  protected def convertMatrixToH2OFrameKeyArray(matrix: Array[DenseMatrix]): Array[String] = {
    convertWithH2OContext(matrix) { (spark, hc) =>
      import spark.implicits._
      matrix.map { matrix =>
        val rows = matrix.rowIter.map(_.toArray).toArray
        val df = spark.sparkContext.parallelize(rows).toDF()
        hc.asH2OFrame(df).frameId
      }
    }
  }
}
