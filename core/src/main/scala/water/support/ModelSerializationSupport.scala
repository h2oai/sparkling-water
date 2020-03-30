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
import org.apache.spark.h2o.H2OBaseModel
import water.H2O
import water.persist.Persist

/**
  * Helper trait containing methods to export and import models from and to Sparkling Water
  */
trait ModelSerializationSupport {

  /**
    * Export binary model to specified directory
    *
    * @param model       model to export
    * @param destination destination URI
    * @param force       override the model if it already exist in the destination URI
    * @return destination URI
    */
  def exportH2OModel(model: H2OBaseModel, destination: URI, force: Boolean = false): URI = {
    model.exportBinaryModel(destination.toString, force)
    destination
  }

  /**
    * Export binary model to specified directory
    *
    * @param model       model to export
    * @param destination destination path
    * @param force       override the model if it already exist in the destination URI
    * @return destination URI
    */
  def exportH2OModel(model: H2OBaseModel, destination: String, force: Boolean): String = {
    exportH2OModel(model, new URI(destination), force).toString
  }

  /**
    * Load H2O binary model from specified directory
    *
    * @param source source URI
    * @tparam M Model Type
    * @return imported model
    */
  def loadH2OModel[M <: H2OBaseModel](source: URI): M = {
    Model.importBinaryModel[M](source.toString)
  }

  /**
    * Load H2O binary model from specified directory
    *
    * @param source source path
    * @tparam M Model Type
    * @return imported model
    */
  def loadH2OModel[M <: H2OBaseModel](source: String): M = {
    Model.importBinaryModel[M](source)
  }

  /**
    * Export POJO model to specified directory
    *
    * @param model       model to export
    * @param destination destination URI
    * @param force       override the model if it already exist in the destination URI
    * @return destination URI
    */
  def exportPOJOModel(model: H2OBaseModel, destination: URI, force: Boolean = false): URI = {
    val p: Persist = H2O.getPM.getPersistForURI(destination)
    val os: OutputStream = p.create(destination.toString, force)
    val writer = new model.JavaModelStreamWriter(false)
    writer.writeTo(os)
    os.close()
    destination
  }

  /**
    * Export POJO model to specified directory
    *
    * @param model       model to export
    * @param destination destination path
    * @param force       override the model if it already exist in the destination URI
    * @return destination URI
    */
  def exportPOJOModel(model: H2OBaseModel, destination: String, force: Boolean): String = {
    exportPOJOModel(model, new URI(destination), force).toString
  }

  /**
    * Export MOJO model to specified directory
    *
    * @param model       model to export
    * @param destination destination URI
    * @param force       override the model if it already exist in the destination URI
    * @return destination URI
    */
  def exportMOJOModel(model: H2OBaseModel, destination: URI, force: Boolean = false): URI = {
    model.exportMojo(destination.toString, force)
    destination
  }

  /**
    * Export MOJO model to specified directory
    *
    * @param model       model to export
    * @param destination destination path
    * @param force       override the model if it already exist in the destination URI
    * @return destination URI
    */
  def exportMOJOModel(model: H2OBaseModel, destination: String, force: Boolean): String = {
    exportMOJOModel(model, new URI(destination), force).toString
  }

  /**
    * Load MOJO model from specified directory
    *
    * @param source source URI
    * @return H2O's Mojo model
    */
  def loadMOJOModel(source: URI): MojoModel = {
    hex.genmodel.MojoModel.load(source.getPath)
  }
}

object ModelSerializationSupport extends ModelSerializationSupport {

  /**
    * Get MOJO from a model
    *
    * @param model model to get MOJO from
    * @return tuple containing MOJO model and binary bytes representing the MOJO
    */
  def getMojo(model: H2OBaseModel): (MojoModel, Array[Byte]) = {
    val mojoData = getMojoData(model)
    val bais = new ByteArrayInputStream(mojoData)
    val reader = MojoReaderBackendFactory.createReaderBackend(bais, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    (ModelMojoReader.readFrom(reader), mojoData)
  }

  /**
    * Get MOJO from a model
    *
    * @param model model to get MOJO from
    * @return MOJO model
    */
  def getMojoModel(model: H2OBaseModel): MojoModel = {
    val mojoData = getMojoData(model)
    val bais = new ByteArrayInputStream(mojoData)
    val reader = MojoReaderBackendFactory.createReaderBackend(bais, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    ModelMojoReader.readFrom(reader)
  }

  /**
    * Get MOJO from a binary representation of MOJO
    *
    * @param mojoData data representing the MOJO model
    * @return MOJO model
    */
  def getMojoModel(mojoData: Array[Byte]): MojoModel = {
    val is = new ByteArrayInputStream(mojoData)
    val reader = MojoReaderBackendFactory.createReaderBackend(is, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    ModelMojoReader.readFrom(reader)
  }

  /**
    * Get MOJO binary representation from a model
    *
    * @param model model to get the binary representation of a MOJO from
    * @return byte array representing the MOJO
    */
  def getMojoData(model: H2OBaseModel): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    model.getMojo.writeTo(baos)
    baos.toByteArray
  }

}
