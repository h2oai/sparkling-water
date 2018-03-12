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
package org.apache.spark.examples.h2o

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.spark.models.svm.{SVM, SVMParameters}
import org.apache.spark.sql.SparkSession
import water.fvec.H2OFrame
import water.support.SparkContextSupport

object SparkSVMDemo extends SparkContextSupport {

  def main(args: Array[String]) {
    val conf = configure("Sparkling Water: Spark SVM demo.")
    val sc = new SparkContext(conf)

    val h2oContext = H2OContext.getOrCreate(sc)
    implicit val sqLContext = SparkSession.builder().getOrCreate().sqlContext

    // Load H2O from CSV file (i.e., access directly H2O cloud)
    // Use super-fast advanced H2O CSV parser !!!
    val breastCancerData = new H2OFrame(new File(TestUtils.locate("smalldata/bcwd.csv")))

    // Training data
    breastCancerData.replace(breastCancerData.numCols()-1, breastCancerData.lastVec().toCategoricalVec)
    breastCancerData.update()

    // Configure Deep Learning algorithm
    val parms = new SVMParameters
    parms._train = breastCancerData.key
    parms._response_column = "label"

    val svm = new SVM(parms, h2oContext)

    val svmModel = svm.trainModel.get

    // Use model for scoring
    val predictionH2OFrame = svmModel.score(breastCancerData)
    val predictionsFromModel = h2oContext.asDataFrame(predictionH2OFrame).collect
    println(predictionsFromModel.mkString("\n===> Model predictions: ", ",\n", ", ...\n"))

    // Stop Spark cluster and destroy all executors
    if (System.getProperty("spark.ext.h2o.preserve.executors")==null) {
      sc.stop()
    }
    // Shutdown H2O
    h2oContext.stop()
  }
}
