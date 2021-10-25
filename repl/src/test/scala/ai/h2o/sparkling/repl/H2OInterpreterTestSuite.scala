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
package ai.h2o.sparkling.repl

import ai.h2o.sparkling.SharedH2OTestContext
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import java.net.URL
import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class H2OInterpreterTestSuite extends FunSuite with SharedH2OTestContext with Matchers {

  private val replLocalJarsPrefix = "spark-repl-local-jars"

  override def createSparkSession(): SparkSession = {
    val tempReplJarsDir = tempDirNotEmpty(replLocalJarsPrefix).toUri.toString
    val conf = defaultSparkConf.set("spark.repl.local.jars", tempReplJarsDir)
    sparkSession("local[*]", conf)
  }

  private var objectUnderTest: H2OInterpreter = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    objectUnderTest = new H2OInterpreter(sc, hc, 1)
  }

  test("should set correct repl related settings") {
    val result = objectUnderTest.createSettings(classLoaderStub(hasAppClassPath = false))
    result.Yreplsync.value shouldBe true
    result.Yreplclassbased.value shouldBe true
    result.Yreploutdir.value shouldBe H2OInterpreter.classOutputDirectory.getAbsolutePath
  }

  test("should use spark.repl.local.jars if available and app.class.path is not set") {
    val result = objectUnderTest.createSettings(classLoaderStub(hasAppClassPath = false))
    result.usejavacp.value shouldBe true
    result.classpath.value should include(replLocalJarsPrefix)
  }

  test("should not set javacp if app.class.path is set") {
    val tmpFilePrefix = "app-class-path"
    val result = objectUnderTest.createSettings(
      classLoaderStub(hasAppClassPath = true, tempDirNotEmpty(tmpFilePrefix).toUri.toURL))
    result.usejavacp.value shouldBe false
    result.classpath.value should startWith(tmpFilePrefix)
  }

  private def classLoaderStub(hasAppClassPath: Boolean, classPathUrl: URL = null) = {
    new ClassLoader() {
      override def getResource(name: String): URL =
        if (name == "app.class.path" && hasAppClassPath) classPathUrl else null
    }
  }

  private def tempDirNotEmpty(filePrefix: String) = {
    val tempDir = Files.createTempDirectory(filePrefix)
    Files.createTempFile(tempDir, filePrefix, "-file")
    tempDir
  }
}
