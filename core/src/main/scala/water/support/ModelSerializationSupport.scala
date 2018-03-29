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
import water.H2O
import water.persist.Persist

trait ModelSerializationSupport {

  def exportH2OModel(model: Model[_, _, _], destination: URI, force: Boolean = false): URI = {
    model.exportBinaryModel(destination.toString, force)
    destination
  }

  def exportH2OModel(model: Model[_, _, _], destination: String, force: Boolean): String = {
    exportH2OModel(model, new URI(destination), force).toString
  }

  def loadH2OModel[M <: Model[_, _, _]](source: URI): M = {
    Model.importBinaryModel[M](source.toString)
  }

  def loadH2OModel[M <: Model[_, _, _]](source: String): M = {
    Model.importBinaryModel[M](source)
  }

  def exportPOJOModel(model: Model[_, _, _], destination: URI, force: Boolean = false): URI = {
    val p: Persist = H2O.getPM.getPersistForURI(destination)
    val os: OutputStream = p.create(destination.toString, force)
    val writer = new model.JavaModelStreamWriter(false)
    writer.writeTo(os)
    os.close()
    destination
  }

  def exportPOJOModel(model: Model[_, _, _], destination: String, force: Boolean): String = {
    exportPOJOModel(model, new URI(destination), force).toString
  }

  def exportMOJOModel(model: Model[_, _, _], destination: URI, force: Boolean = false): URI = {
    model.exportMojo(destination.toString, force)
    destination
  }

  def exportMOJOModel(model: Model[_, _, _], destination: String, force: Boolean): String = {
    exportMOJOModel(model, new URI(destination), force).toString
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
    exportMojo(model, baos)
    baos.toByteArray
  }

  /**
    * Exports the model mojo bytes to an (local) file.
    *
    * The file can then be used to create the mojo using ModelSerializationSupport (thus with sparkling-water):
    * <pre>
    * {@code
    * ModelSerializationSupport.exportMojo(model, file)
    * val bytes = java.nio.file.Files.readAllBytes(file.toPath)
    * val mojo = ModelSerializationSupport.getMojoModel(bytes)
    * }
    * </pre>
    *
    * The file can also be used to create a mojo model without sparkling water (with just h2o-genmodel)
    * <pre>
    * {@code
    * ModelSerializationSupport.exportMojo(model, file)
    * val is = new FileInputStream(file)
    * val reader = MojoReaderBackendFactory.createReaderBackend(is, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    * val mojoModel = ModelMojoReader.readFrom(reader)
    * }
    * </pre>
    *
    * Note that this does not generate a .zip file, which is expected by the `MojoModel.load(path)`
    * @param model The model to serialize
    * @param file  The location to write the model to
    */
  def exportMojo(model: Model[_, _, _], file: File) = {
    val outputStream = new FileOutputStream(file)
    try {
      exportMojo(model, outputStream)
      model.getMojo.writeTo(outputStream)
    }
    finally if (outputStream != null) outputStream.close()
  }

  /**
    * Exports the model mojo bytes to an outputstream.
    *
    * The byte array can then be used to create the mojo using ModelSerializationSupport (thus with sparkling-water):
    * <pre>
    * {@code
    * val baos = new ByteArrayOutputStream()
    * ModelSerializationSupport.exportMojo(baos)
    * val mojo = ModelSerializationSupport.getMojoModel(baos.toByteArray)
    * }
    * </pre>
    *
    * The byte array can also be used to create a mojo model without sparkling water (with just h2o-genmodel)
    * <pre>
    * {@code
    * val baos = new ByteArrayOutputStream()
    * ModelSerializationSupport.exportMojo(baos)
    * val is = new ByteArrayInputStream(baos.toByteArray)
    * val reader = MojoReaderBackendFactory.createReaderBackend(is, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    * val mojoModel = ModelMojoReader.readFrom(reader)
    * }
    * </pre>
    * @param model The model to serialize
    * @param baos  The destination to send the serialization bytes to
    */
  def exportMojo(model: Model[_, _, _], baos: OutputStream) = {
    model.getMojo.writeTo(baos)
  }

}
