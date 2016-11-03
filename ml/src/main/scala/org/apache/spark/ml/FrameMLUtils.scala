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
package org.apache.spark.ml

import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.spark.models.MissingValuesHandling
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField}
import water.fvec.{Frame, H2OFrame}

object FrameMLUtils {
  /** 
    * Converts a H2O Frame into an RDD[LabeledPoint]. Assumes that the last column is the response column which will be used 
    * as the label. All other columns will be mapped to a vector and used as features. Categorical columns will be mapped to their 
    * numerical values. 
    * 
    * @param frame Input frame to be converted 
    * @param reponseColumn Column which contains the labels 
    * @param nfeatures Number of features we want to use 
    * @param missingHandler Missing values strategy 
    * @param h2oContext Current H2OContext 
    * @param sqlContext Current SQLContext 
    * @return Returns an equivalent RDD[LabeledPoint] and means for each column 
    */
  def toLabeledPoints(frame: Frame,
                      reponseColumn: String,
                      nfeatures: Int,
                      missingHandler: MissingValuesHandling,
                      h2oContext: H2OContext,
                      sqlContext: SQLContext): (RDD[LabeledPoint], Array[Double]) = {
    var means: Array[Double] = new Array[Double](nfeatures)
    val domains = frame.domains()

    val trainingDF = h2oContext.asDataFrame(new H2OFrame(frame))(sqlContext)
    val fields: Array[StructField] = trainingDF.schema.fields
    var trainingRDD = trainingDF.rdd

    if (MissingValuesHandling.Skip.eq(missingHandler)) {
      trainingRDD = trainingRDD.filter(!_.anyNull)
    } else if (MissingValuesHandling.MeanImputation.eq(missingHandler)) {
      // Computing the means by hand and not using frame.means() as it does not compute the mean for enum columns
      means = movingAverage(trainingRDD, fields, domains)
    }

    (trainingRDD.map(row => {
      val features = (0 until nfeatures).map{ i =>
        if (row.isNullAt(i)) means(i)
        else toDouble(row.get(i), fields(i), domains(i))
      }.toArray[Double]

      new LabeledPoint(
        toDouble(row.getAs[String](reponseColumn), fields(fields.length - 1), domains(domains.length - 1)),
        Vectors.dense(features)
      )
    }), means)
  }

  // Running average so we don't get overflows
  private[ml] def movingAverage(trainingRDD: RDD[Row],
                                fields: Array[StructField],
                                domains: Array[Array[String]]): Array[Double] = {
    val means = new Array[Double](fields.length)
    val counts = new Array[Int](means.length)
    trainingRDD.aggregate(means.zip(counts))(
      // Compute the average within a RDD partition
      (agg, row) => {
        agg.indices.foreach(i => {
          if (!row.isNullAt(i)) {
            val value = toDouble(row.get(i), fields(i), domains(i))
            val delta = value - agg(i)._1
            val n = agg(i)._2 + 1
            agg(i) = (agg(i)._1 + delta / n, n)
          }
        })
        agg
      },
      // Merge all RDD partition stats
      (agg1, agg2) => {
        merge(agg1, agg2)
      }
    ).map(_._1)
  }

  def merge(agg1: Array[(Double, Int)], agg2: Array[(Double, Int)]): Array[(Double, Int)] = {
    agg1.indices.foreach { idx =>
      if (agg1(idx)._2 == 0) {
        agg1(idx) = agg2(idx)
      } else {
        val otherMu: Double = agg2(idx)._1
        val mu: Double = agg1(idx)._1
        val n = agg1(idx)._2
        val otherN = agg2(idx)._2
        val delta = otherMu - mu

        if (otherN * 10 < n) {
          agg1(idx) = (mu + (delta * otherN) / (n + otherN), n + otherN)
        } else if (n * 10 < otherN) {
          agg1(idx) = (otherMu - (delta * n) / (n + otherN), n + otherN)
        } else {
          agg1(idx) = ((mu * n + otherMu * otherN) / (n + otherN), n + otherN)
        }
      }
    }
    agg1
  }

  private[ml] def toDouble(value: Any, fieldStruct: StructField, domain: Array[String]): Double = {
    value match {
      case b: Byte if fieldStruct.dataType == DataTypes.ByteType => b.doubleValue
      case s: Short if fieldStruct.dataType == DataTypes.ShortType => s.doubleValue
      case i: Int if fieldStruct.dataType == DataTypes.IntegerType => i.doubleValue
      case d: Double if fieldStruct.dataType == DataTypes.DoubleType => d
      case string: String if fieldStruct.dataType == DataTypes.StringType => domain.indexOf(string)
      case _ => throw new IllegalArgumentException("Target column has to be an enum or a number. " + fieldStruct)
    }
  }
}
