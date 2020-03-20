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
package water.support

import java.io.File
import java.net.URI

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

/**
 * Useful methods to configure Spark context
 */
trait SparkContextSupport {

  /**
   * Helper method to configure basic spark settings
   *
   * @param appName       application name
   * @param defaultMaster master string
   * @return Spark configuration
   */
  def configure(appName: String = "Sparkling Water Demo", defaultMaster: String = "local[*]"): SparkConf = {
    val conf = new SparkConf()
      .setAppName(appName)
    conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", defaultMaster))
    conf
  }

  /**
   * Get or create spark context
   *
   * @param conf spark configuration
   * @return Spark context
   */
  def sparkContext(conf: SparkConf) = SparkContext.getOrCreate(conf)

  /**
   * Returns true if the file has already been added to Spark files
   *
   * @param sc       Spark context
   * @param filePath any path containing the file name
   * @return true if the file has already been added to Spark files, otherwise false
   */
  def isFileDistributed(sc: SparkContext, filePath: String): Boolean = {
    val fileName = new File(filePath).getName
    sc.listFiles().filter(new File(_).getName == fileName).nonEmpty
  }

  /**
   * Add files into Spark context
   *
   * @param sc    Spark context
   * @param files path to files to add
   */
  def addFiles(sc: SparkContext, files: String*): Unit = {
    files.foreach(f => sc.addFile(f))
  }

  /**
   * Add files into Spark context
   *
   * @param spark Spark session
   * @param files path to files to add
   */
  def addFiles(spark: SparkSession, files: String*): Unit = addFiles(spark.sparkContext, files: _*)

  /**
   * This method enforces the local path. For example, if running on Hadoop, Spark by default uses HDFS. If we
   * need to access a local file, for example, from driver, then this method might be useful
   *
   * @param file path to the file
   * @return local path to the file
   */
  def enforceLocalSparkFile(file: String): String = {
    "file://" + SparkFiles.get(file)
  }

  /**
   * Return absolute path of a file location
   *
   * @param path path to a file
   * @return absolute path to a file
   */
  def absPath(path: String): String = new java.io.File(path).getAbsolutePath

  /**
   * Export Spark Model to a specified destination URI
   *
   * @param model       Spark model
   * @param destination destination URI
   */
  def exportSparkModel(model: Any, destination: URI): Unit = {
    import java.io.{FileOutputStream, ObjectOutputStream}
    val fos = new FileOutputStream(new File(destination))
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(model)
    oos.close()
  }

  /**
   * Import Spark Model from a specified source URI
   *
   * @param source source URI
   * @tparam M Model Type
   * @return the loaded model
   */
  def loadSparkModel[M](source: URI): M = {
    import java.io.{FileInputStream, ObjectInputStream}
    val fos = new FileInputStream(new File(source))
    val oos = new ObjectInputStream(fos)
    val newModel = oos.readObject().asInstanceOf[M]
    newModel
  }
}

object SparkContextSupport extends SparkContextSupport
