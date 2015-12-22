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
package org.apache.spark.repl

import java.net.URL

import org.apache.spark.util.MutableURLClassLoader
import org.apache.spark.{SparkEnv, SparkContext}

import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader

/**
 * Various helper methods for classloaders used in the REPL environment
 */
class ClassLoaderHelper(val sc: SparkContext) {
  private var _replClassLoader: AbstractFileClassLoader = null
  private var _runtimeClassLoader: URLClassLoader with ExposeAddUrl = null // wrapper exposing addURL

  if(sc.isLocal){
    prepareLocalClassLoader()
  }else{
    prepareRemoteClassLoader()
  }


  private def prepareRemoteClassLoader() = {
    if (Main.interp != null) {
      setClassLoaderToSerializers(Main.interp.intp.classLoader)
      _replClassLoader = Main.interp.intp.classLoader
    } else {
      _replClassLoader = null
    }
  }

  /**
   * Add directory with classes defined in REPL to the classloader
   which is used in the local mode. This classloader is obtained using reflections.
   */
  private def prepareLocalClassLoader() =  {
    val f  =  SparkEnv.get.serializer.getClass.getSuperclass.getDeclaredField("defaultClassLoader")
    f.setAccessible(true)
    val value =  f.get(SparkEnv.get.serializer)
    value match {
      case v : Option[_] => {
        v.get match {
          case cl: MutableURLClassLoader => cl.addURL(REPLClassServerUtils.getClassOutputDir.toURI.toURL)
          case _ =>
        }
      }
      case _ =>
    }
  }

  private def setClassLoaderToSerializers(classLoader: ClassLoader): Unit ={
    SparkEnv.get.serializer.setDefaultClassLoader(classLoader)
    SparkEnv.get.closureSerializer.setDefaultClassLoader(classLoader)
  }

  def REPLCLassLoader = this.synchronized{
    _replClassLoader
  }

  def ensureREPLClassLoader(classLoader: AbstractFileClassLoader) = this.synchronized{
    if(_replClassLoader == null) {
      _replClassLoader = classLoader
      setClassLoaderToSerializers(_replClassLoader)
    }
  }
  def resetREPLCLassLoader() : Unit = this.synchronized{
    _replClassLoader = null
  }

  def runtimeClassLoader = this.synchronized{
    _runtimeClassLoader
  }
  def ensureRuntimeCLassLoader(classLoader: URLClassLoader with ExposeAddUrl) = this.synchronized{
    if(_runtimeClassLoader == null){
      _runtimeClassLoader = classLoader
    }
  }

  def addUrlsToClasspath(urls: URL*): Unit = {
    if (Main.interp != null) {
      Main.interp.intp.addUrlsToClassPath(urls: _*)
    } else {
      urls.foreach(_runtimeClassLoader.addNewUrl)
    }
  }
}
