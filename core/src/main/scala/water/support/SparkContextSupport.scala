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

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

/**
 * Publish useful method to configure Spark context.
 */
trait SparkContextSupport {

  def configure(appName: String = "Sparkling Water Demo", defaultMaster: String = "local[*]"): SparkConf = {
    val conf = new SparkConf()
      .setAppName(appName)
    conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", defaultMaster))
    conf
  }

  def sparkContext(conf: SparkConf) = SparkContext.getOrCreate(conf)

  def addFiles(sc: SparkContext, files: String*): Unit = {
    files.foreach(f => sc.addFile(f))
  }

  def enforceLocalSparkFile(file: String): String ={
    "file://" + SparkFiles.get(file)
  }

  def absPath(path: String): String = new java.io.File(path).getAbsolutePath

  def exportSparkModel(model: Any, destination: URI): Unit = {
    import java.io.{FileOutputStream, ObjectOutputStream}
    val fos = new FileOutputStream(new File(destination))
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(model)
    oos.close()
  }

  def loadSparkModel[M](source: URI) : M = {
    import java.io.{FileInputStream, ObjectInputStream}
    val fos = new FileInputStream(new File(source))
    val oos = new ObjectInputStream(fos)
    val newModel = oos.readObject().asInstanceOf[M]
    newModel
  }
}
object SparkContextSupport extends SparkContextSupport
