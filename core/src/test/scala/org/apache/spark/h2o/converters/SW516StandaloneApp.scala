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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.sql.SQLContext

object SW516StandaloneApp {
  val valuesCnt = 10
  val partitions = 2
  val cloudName = "SW516"
}

case class Data(f1: org.apache.spark.mllib.linalg.Vector,
                f2: org.apache.spark.mllib.linalg.Vector)

object SWApp {
  import SW516StandaloneApp._

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("SW516")
      .setMaster("local[*]")
      .set("spark.ext.h2o.external.read.confirmation.timeout", "240")
    val sc = new SparkContext("local[*]", "test-local", conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val h2oConf = new H2OConf(sc).setExternalClusterMode().useManualClusterStart().setCloudName(cloudName)
    val hc = H2OContext.getOrCreate(sc, h2oConf)

    val values = (0 until valuesCnt).map(x =>
     Data(
       org.apache.spark.mllib.linalg.Vectors.sparse(valuesCnt, Seq((x, 1.0))),
       org.apache.spark.mllib.linalg.Vectors.dense(x.toDouble, 0.0, 1.0, 42.0)
     ))

    import sqlContext.implicits._
    println(s"Values to transfer: ${values.mkString("\n")}")
    
    // Create data in Spark
    val df = sc.parallelize(values, partitions).toDF()
    df.printSchema()

    // Transfer data to H2O
    val hf = hc.asH2OFrame(df)
    println(hf.toString(0, 100))

    sc.stop()
  }
}

object H2OApp {
  import SW516StandaloneApp._

  def main(args: Array[String]): Unit = {
    water.H2O.main(Array("-name", cloudName, "-md5skip"))
  }

}


