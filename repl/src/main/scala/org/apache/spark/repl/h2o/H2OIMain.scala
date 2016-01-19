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

package org.apache.spark.repl.h2o

import org.apache.spark.util.MutableURLClassLoader
import org.apache.spark.{SparkContext, SparkEnv, HttpServer}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.repl.{Main, SparkIMain}

import scala.collection.mutable
import scala.reflect.internal.util.BatchSourceFile
import scala.reflect.io.PlainFile
import scala.tools.nsc.interpreter.AbstractOrMissingHandler
import scala.tools.nsc.{Global, Settings, io}

/**
  * SparkIMain which allows parallel interpreters to coexist. It adds each class to package which specifies the interpreter
  * where this class was declared
  */
private[repl] class H2OIMain private(initialSettings: Settings,
               interpreterWriter: IntpResponseWriter,
               val sessionID: Int,
               propagateExceptions: Boolean = false) extends SparkIMain(initialSettings, interpreterWriter, propagateExceptions){

  private val _compiler: Global = getAndSetCompiler()
  stopClassServer()
  setupClassNames()

  @DeveloperApi
  override def initializeSynchronous(): Unit = {
    val _initializedCompleteField = this.getClass.getSuperclass.getDeclaredField("_initializeComplete")
    _initializedCompleteField.setAccessible(true)
    if (!_initializedCompleteField.get(this).asInstanceOf[Boolean]) {
      _initialize()
      assert(global != null, global)
    }
  }

  private def _initialize() ={
    try {
      // todo. if this crashes, REPL will hang
      new _compiler.Run().compileSources(_initSources)
      val _initializedCompleteField = this.getClass.getSuperclass.getDeclaredField("_initializeComplete")
      _initializedCompleteField.setAccessible(true)
      _initializedCompleteField.set(this,true)
      true
    }
    catch AbstractOrMissingHandler()

  }

  private def _initSources = List(new BatchSourceFile("<init>", "package intp_id_" + sessionID + " \n class $repl_$init { }"))

  /**
    * Stop class server started in SparkIMain constructor because we have already one running and need to use
    * that server
    */
  private def stopClassServer(): Unit ={
    val fieldClassServer =  this.getClass.getSuperclass.getDeclaredField("classServer")
    fieldClassServer.setAccessible(true)
    val classServer = fieldClassServer.get(this).asInstanceOf[HttpServer]
    classServer.stop()
  }

  private def getAndSetCompiler(): Global = {
    // need to override virtualDirectory so it uses our directory
    val fieldVirtualDirectory = this.getClass.getSuperclass.getDeclaredField("org$apache$spark$repl$SparkIMain$$virtualDirectory")
    fieldVirtualDirectory.setAccessible(true)
    fieldVirtualDirectory.set(this,new PlainFile(H2OInterpreter.classOutputDir))

    // need to initialize the compiler again so it uses new virtualDirectory
    val fieldCompiler = this.getClass.getSuperclass.getDeclaredField("_compiler")
    fieldCompiler.setAccessible(true)
    fieldCompiler.set(this,newCompiler(settings,reporter))
    fieldCompiler.get(this).asInstanceOf[Global]
  }

  /**
    * Ensures that before each class is added special prefix identifying the interpreter
    * where this class was declared
    */
  private def setupClassNames(): Unit ={
    val ru = scala.reflect.runtime.universe
    val mirror = ru.runtimeMirror(getClass.getClassLoader)

    val fixedSessionNamesSymbol = ru.typeOf[FixedSessionNames.type].termSymbol.asModule

    val moduleMirror = mirror.reflect(this).reflectModule(fixedSessionNamesSymbol)
    val instanceMirror = mirror.reflect(moduleMirror.instance)

    val lineNameTerm = ru.typeOf[FixedSessionNames.type].declaration(ru.newTermName("lineName")).asTerm.accessed.asTerm
    val fieldMirror = instanceMirror.reflectField(lineNameTerm)

    val previousValue = fieldMirror.get.asInstanceOf[String]
    fieldMirror.set("intp_id_" + sessionID + "." + previousValue)
  }

  override private[repl] def initialize(postInitSignal: => Unit): Unit = {
    val _isInitializedField = this.getClass.getSuperclass.getDeclaredField("_isInitialized")
    _isInitializedField.setAccessible(true)
    synchronized {
      if (_isInitializedField.get(this) == null) {
        _isInitializedField.set(this,io.spawn {
          try _initialize()
          finally postInitSignal
        })
      }
    }
  }
}

object H2OIMain {
  val existingInterpreters = mutable.HashMap.empty[Int, H2OIMain]
  private var interpreterClassloader: InterpreterClassLoader = _
  private var _initialized = false

  private def setClassLoaderToSerializers(classLoader: ClassLoader): Unit = {
    SparkEnv.get.serializer.setDefaultClassLoader(classLoader)
    SparkEnv.get.closureSerializer.setDefaultClassLoader(classLoader)
  }

  /**
    * Add directory with classes defined in REPL to the classloader
   which is used in the local mode. This classloader is obtained using reflections.
    */
  private def prepareLocalClassLoader() = {
    val f = SparkEnv.get.serializer.getClass.getSuperclass.getDeclaredField("defaultClassLoader")
    f.setAccessible(true)
    val value = f.get(SparkEnv.get.serializer)
    value match {
      case v: Option[_] => {
        v.get match {
          case cl: MutableURLClassLoader => cl.addURL(H2OInterpreter.classOutputDir.toURI.toURL)
          case _ =>
        }
      }
      case _ =>
    }
  }

  private def initialize(sc: SparkContext): Unit = {
    if (sc.isLocal) {
      // using master set to local or local[*]
      prepareLocalClassLoader()
      interpreterClassloader = new InterpreterClassLoader()
    } else {
      if (Main.interp != null) {
        interpreterClassloader = new InterpreterClassLoader(Main.interp.intp.classLoader)
      } else {
        // non local mode, application not started using SparkSubmit
        interpreterClassloader = new InterpreterClassLoader()
      }
    }
    setClassLoaderToSerializers(interpreterClassloader)
  }
  def getInterpreterClassloader: InterpreterClassLoader = {
    interpreterClassloader
  }

  def createInterpreter(sc: SparkContext, settings: Settings, interpreterWriter: IntpResponseWriter, sessionId: Int): H2OIMain = synchronized {
    if(!_initialized){
      initialize(sc)
      _initialized = true
    }
    existingInterpreters += (sessionId -> new H2OIMain(settings, interpreterWriter, sessionId, false))
    existingInterpreters(sessionId)
  }
}
