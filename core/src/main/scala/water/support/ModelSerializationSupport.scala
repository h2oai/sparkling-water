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

import java.io.{FileOutputStream, File, InputStream, OutputStream}
import java.net.URI

import hex.Model
import water.persist.Persist
import water.{AutoBuffer, H2O, Keyed, Key}

trait ModelSerializationSupport {

  def exportH2OModel(model : Model[_,_,_], destination: URI): URI = {
    val modelKey = model._key.asInstanceOf[Key[_ <: Keyed[_ <: Keyed[_ <: AnyRef]]]]
    val p: Persist = H2O.getPM.getPersistForURI(destination)
    val os: OutputStream = p.create(destination.toString, true)
    model.writeAll(new AutoBuffer(os, true)).close

    destination
  }

  def loadH2OModel[M <: Model[_, _, _]](source: URI) : M = {
    val p: Persist = H2O.getPM.getPersistForURI(source)
    val is: InputStream = p.open(source.toString)
    Keyed.readAll(new AutoBuffer(is)).asInstanceOf[M]
  }

  def exportPOJOModel(model : Model[_, _,_], destination: URI): URI = {
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
}

object ModelSerializationSupport extends ModelSerializationSupport
