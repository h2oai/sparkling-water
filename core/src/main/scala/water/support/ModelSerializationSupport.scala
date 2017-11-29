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

import java.io._
import java.net.URI

import hex.Model
import hex.genmodel.{ModelMojoReader, MojoModel, MojoReaderBackendFactory}
import water.persist.Persist
import water.{AutoBuffer, H2O, Key, Keyed}
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}

trait ModelSerializationSupport {

  def exportH2OModel(model: Model[_, _, _], destination: URI): URI = {
    val modelKey = model._key.asInstanceOf[Key[_ <: Keyed[_ <: Keyed[_ <: AnyRef]]]]
    val p: Persist = H2O.getPM.getPersistForURI(destination)
    val os: OutputStream = p.create(destination.toString, true)
    model.writeAll(new AutoBuffer(os, true)).close

    destination
  }

  def loadH2OModel[M <: Model[_, _, _]](source: URI): M = {
    val p: Persist = H2O.getPM.getPersistForURI(source)
    val is: InputStream = p.open(source.toString)
    Keyed.readAll(new AutoBuffer(is)).asInstanceOf[M]
  }


  def exportPOJOModel(model: Model[_, _, _], destination: URI): URI = {
    val destFile = new File(destination)
    val fos = new FileOutputStream(destFile)
    val writer = new model.JavaModelStreamWriter(false)
    try {
      writer.writeTo(fos)
    } finally {
      fos.close()
    }
    destination
  }

  def exportMOJOModel(model: Model[_, _, _], destination: URI): URI = {
    val destFile = new File(destination)
    val fos = new FileOutputStream(destFile)
    model.getMojo.writeTo(fos)
    destination
  }

  def exportMOJOModelToHDFS(model: Model[_, _, _], destination: URI, sc: SparkContext): URI = {
    try {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val output = fs.create(new Path(destination))
    val os = new BufferedOutputStream(output)
    model.getMojo.writeTo(os)
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException (s"Error while writing to HDFS (most likely no HDFS is available) in destination $destination : ${e.getMessage}")
    }
    destination
  }

  def loadMOJOModel(source: URI): MojoModel = {
    hex.genmodel.MojoModel.load(source.getPath)
  }
}

object ModelSerializationSupport extends ModelSerializationSupport {

  def getMojo(model: Model[_, _, _]): (MojoModel, Array[Byte]) = {
    val mojoData = getMojoData(model)
    val bais = new ByteArrayInputStream(mojoData)
    val reader = MojoReaderBackendFactory.createReaderBackend(bais, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    (ModelMojoReader.readFrom(reader), mojoData)
  }

  def getMojoModel(model: Model[_, _, _]) = {
    val mojoData = getMojoData(model)
    val bais = new ByteArrayInputStream(mojoData)
    val reader = MojoReaderBackendFactory.createReaderBackend(bais, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    ModelMojoReader.readFrom(reader)
  }

  def getMojoModel(mojoData: Array[Byte]) = {
    val is = new ByteArrayInputStream(mojoData)
    val reader = MojoReaderBackendFactory.createReaderBackend(is, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    ModelMojoReader.readFrom(reader)
  }

  def getMojoData(model: Model[_, _, _]) = {
    val baos = new ByteArrayOutputStream()
    model.getMojo.writeTo(baos)
    baos.toByteArray
  }

}
