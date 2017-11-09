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

package org.apache.spark.h2o.utils

import java.security.Permission

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import org.scalatest.Suite

/**
  * Helper trait to simplify initialization and termination of H2O contexts.
  *
  */
trait SharedH2OTestContext extends SparkTestContext {
  self: Suite =>

  def createSparkContext: SparkContext

  @transient var hc: H2OContext = _

  override def beforeAll() {
    super.beforeAll()
    sc = createSparkContext
    hc = H2OContextTestHelper.createH2OContext(sc, 2)
  }

  override def afterAll() {
    // The method H2O.exit calls System.exit which confuses Gradle and marks the build
    // as successful even though some tests failed.
    // We can solve this by using security manager which forbids System.exit call.
    // It is safe to use as all the methods closing H2O cloud and stopping operations have been
    // already called and we just need to ensure that JVM with the client/driver doesn't call the System.exit method
    try {
      val securityManager = new NoExitCheckSecurityManager
      System.setSecurityManager(securityManager)
      H2OContextTestHelper.stopH2OContext(sc, hc)
      hc = null
      resetSparkContext()
      super.afterAll()
      System.setSecurityManager(null)
    } catch {
      case _: SecurityException => // ignore
    }
  }

  private class NoExitCheckSecurityManager extends SecurityManager {
    override def checkPermission(perm: Permission): Unit = {
      /* allow any */
    }

    override def checkPermission(perm: Permission, context: scala.Any): Unit = {
      /* allow any */
    }

    override def checkExit(status: Int): Unit = {
      super.checkExit(status)
      throw new SecurityException()
    }
  }

}

