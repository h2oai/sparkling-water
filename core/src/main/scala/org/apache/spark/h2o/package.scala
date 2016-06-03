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

package org.apache.spark

import org.apache.spark.sql._

/** Type shortcuts to simplify work in Sparkling REPL */
package object h2o {
  type Frame = water.fvec.Frame
  //type Key = water.Key
  type H2O = water.H2O

  // Alias for H2OFrame
  type H2OFrame = water.fvec.H2OFrame
  // Alias for
  type RDD[X] = org.apache.spark.rdd.RDD[X]

  type Dataset[X] = org.apache.spark.sql.Dataset[X]

  trait Holder[T] {
    def result: Option[T]
  }

  case class ByteHolder  (result: Option[Byte])   extends Holder[Byte]
  case class DoubleHolder(result: Option[Double]) extends Holder[Double]
  case class IntHolder   (result: Option[Int])    extends Holder[Int]
  case class ShortHolder (result: Option[Short])  extends Holder[Short]
  case class StringHolder(result: Option[String]) extends Holder[String]


  /**
  * Adds a method, `h2o`, to DataFrameWriter that allows you to write h2o frames using
    * the DataFileWriter. It's alias for sqlContext.write.format("org.apache.spark.h2o").option("key","new_frame_key").save()
  */
  implicit class H2ODataFrameWriter[T](writer: DataFrameWriter[T]) {
    def h2o(key: String): Unit = writer.format("org.apache.spark.h2o").save(key)
    def h2o(key: water.Key[_]): Unit = h2o(key.toString)
  }

  /**
    * Adds a method, `h2o`, to DataFrameReader that allows you to read h2o frames using
    * the DataFileReader. It's alias for sqlContext.read.format("org.apache.spark.h2o").option("key",frame.key.toString).load()
    */
  implicit class H2ODataFrameReader(reader: DataFrameReader) {
    def h2o(key: String): DataFrame = reader.format("org.apache.spark.h2o").load(key)
    def h2o(key: water.Key[_]): DataFrame = h2o(key.toString)
  }

}
