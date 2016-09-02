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

package org.apache.spark.h2o.converters

import org.apache.spark.h2o._
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{Logging, TaskContext}
import water.Key
import water.fvec.{H2OFrame, Vec}

import scala.collection.immutable
import scala.language.implicitConversions
import language.postfixOps

private[converters] object LabeledPointConverter extends Logging with ConverterUtils{

  /** Transform RDD[LabeledPoint] to appropriate H2OFrame */
  def toH2OFrame(hc: H2OContext, rdd: RDD[LabeledPoint], frameKeyName: Option[String]): H2OFrame = {

    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand())

    // first convert vector to dense vector
    val rddDense = rdd.map(labeledPoint => new LabeledPoint(labeledPoint.label,labeledPoint.features.toDense))
    val numFeatures = rddDense.map(labeledPoint => labeledPoint.features.size)
    val maxNumFeatures = numFeatures.max()
    val minNumFeatures = numFeatures.min()
    if(minNumFeatures<maxNumFeatures){
      // Features vectors of different sizes, filling missing with n/a
      logWarning("WARNING: Converting RDD[LabeledPoint] to H2OFrame where features vectors have different size, filling missing with n/a")
    }
    val fnames = ("label" :: 0.until(maxNumFeatures).map("feature" +).toList).toArray[String]
    val vecTypes = Array.fill(maxNumFeatures + 1)(Vec.T_NUM)

    convert[LabeledPoint](hc, rdd, keyName, fnames, vecTypes, perLabeledPointRDDPartition(maxNumFeatures))
  }

  /**
    *
    * @param keyName key of the frame
    * @param vecTypes h2o vec types
    * @param maxNumFeatures maximum number of features in the labeled point
    * @param uploadPlan plan which assigns each partition h2o node where the data from that partition will be uploaded
    * @param context spark task context
    * @param it iterator over data in the partition
    * @return pair (partition ID, number of rows in this partition)
    */
  private[this]
  def perLabeledPointRDDPartition(maxNumFeatures: Int)
                                 (keyName: String, vecTypes: Array[Byte], uploadPlan: Option[immutable.Map[Int, NodeDesc]])
                                 (context: TaskContext, it: Iterator[LabeledPoint]): (Int, Long) = {
    val con = ConverterUtils.getWriteConverterContext(uploadPlan, context.partitionId())

    // Creates array of H2O NewChunks; A place to record all the data in this partition
    con.createChunks(keyName, vecTypes, context.partitionId())

    it.foreach(labeledPoint => {
      // For all LabeledPoints in RDD
      var nextChunkId = 0

      // Add LabeledPoint label
      con.put(nextChunkId, labeledPoint.label)
      nextChunkId = nextChunkId + 1

      for( i<-0 until labeledPoint.features.size) {
        // For all features...
        con.put(nextChunkId, labeledPoint.features(i))
        nextChunkId = nextChunkId + 1
      }

      for( i<-labeledPoint.features.size until maxNumFeatures){
        // Fill missing features with n/a
        con.putNA(nextChunkId)
        nextChunkId = nextChunkId + 1
      }

      con.increaseRowCounter()
    })

    //Compress & write data in partitions to H2O Chunks
    con.closeChunks()

    // Return Partition number and number of rows in this partition
    (context.partitionId, con.numOfRows)
  }
}
