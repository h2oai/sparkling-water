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

import org.apache.spark.SparkEnv

import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader

/**
 * Various utils needed to work for the interpreters. Mainly this object is used to register repl classloader, which is
 * shared among all the interpreters.
 * For the first time the H2OIMain is initialized, id creates the repl classloader and stores it here. Other instances
 * of H2OIMain then obtain the classloader from here.
 */


object InterpreterUtils {
  private var _replClassLoader: AbstractFileClassLoader = {
    if (Main.interp != null) {
      SparkEnv.get.serializer.setDefaultClassLoader(Main.interp.intp.classLoader)
      SparkEnv.get.closureSerializer.setDefaultClassLoader(Main.interp.intp.classLoader)
      Main.interp.intp.classLoader
    } else {
      null
    }
  }
  private var _runtimeClassLoader: URLClassLoader with ExposeAddUrl = null // wrapper exposing addURL

  def getClassOutputDir = {
    if (Main.interp != null) {
      Main.interp.intp.getClassOutputDirectory
    } else {
    ReplCLassServer.getClassOutputDirectory
    }
  }

  def classServerUri = {
    if (Main.interp != null) {
      Main.interp.intp.classServerUri
    } else {
      if (!ReplCLassServer.isRunning) {
        ReplCLassServer.start()
      }
    ReplCLassServer.classServerUri
  }
  }


  def REPLCLassLoader = this.synchronized{
    _replClassLoader
  }
  def ensureREPLClassLoader(classLoader: AbstractFileClassLoader) = this.synchronized{
    if(_replClassLoader == null) {
      _replClassLoader = classLoader
      SparkEnv.get.serializer.setDefaultClassLoader(_replClassLoader)
      SparkEnv.get.closureSerializer.setDefaultClassLoader(_replClassLoader)
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

private[repl] trait ExposeAddUrl extends URLClassLoader {
  def addNewUrl(url: URL) = this.addURL(url)
}