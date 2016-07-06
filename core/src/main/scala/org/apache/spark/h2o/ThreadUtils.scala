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
package org.apache.spark.h2o

/**
  * Helper to play nicely with threads.
  *
  * Motivated by Play code: https://github.com/playframework/playframework/blob/master/framework/src/play/src/main/scala/play/utils/Threads.scala
  */
object ThreadUtils {
  /**
    * Executes given function in the context of the provided classloader
    * @param classloader that should be used to execute given function
    * @param b function to be executed
    */
  def withContextClassLoader[T](classloader: ClassLoader)(b: => T): T = {
    val thread = Thread.currentThread
    val oldLoader = thread.getContextClassLoader
    try {
      thread.setContextClassLoader(classloader)
      b
    } finally {
      thread.setContextClassLoader(oldLoader)
    }
  }

}
