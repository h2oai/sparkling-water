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
package ai.h2o.sparkling

import java.lang.reflect.Modifier

import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.backend.external.ExternalBackendConf
import ai.h2o.sparkling.backend.internal.InternalBackendConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

/**
  * Test passing parameters via SparkConf.
  */
@RunWith(classOf[JUnitRunner])
class H2OConfTestSuite extends FunSuite with SparkTestContext with Matchers {

  override def createSparkSession(): SparkSession = sparkSession("local[*]", createConf())

  test("test H2OConf parameters") {
    val conf = new H2OConf().setClusterSize(1)

    assert(conf.numH2OWorkers.contains(42))
    assert(conf.basePort == 32333)
    assert(conf.clientIp.contains("10.0.0.100"))
    assert(conf.cloudTimeout == 10 * 1000)
    assert(conf.numRddRetries == 2)
    assert(conf.cloudName.isDefined)
    assert(conf.cloudName.contains("test-sparkling-cloud-"))
    assert(conf.logLevel == "DEBUG")
    assert(conf.clientNetworkMask.contains("127.0.0.1/32"))
    assert(conf.nodeNetworkMask.contains("0.0.0.1/24"))
    assert(conf.nthreads == 7)
    assert(conf.clientWebPort == 13321)
    assert(conf.drddMulFactor == 2)
  }

  private def createConf() = {
    new SparkConf()
      .set("spark.ext.h2o.cluster.size", "42")
      .set("spark.ext.h2o.client.ip", "10.0.0.100")
      .set("spark.ext.h2o.base.port", "32333")
      .set("spark.ext.h2o.cloud.timeout", (10 * 1000).toString)
      .set("spark.ext.h2o.spreadrdd.retries", "2")
      .set("spark.ext.h2o.cloud.name", "test-sparkling-cloud-")
      .set("spark.ext.h2o.log.level", "DEBUG")
      .set("spark.ext.h2o.client.network.mask", "127.0.0.1/32")
      .set("spark.ext.h2o.node.network.mask", "0.0.0.1/24")
      .set("spark.ext.h2o.nthreads", "7")
      .set("spark.ext.h2o.client.web.port", "13321")
      .set("spark.ext.h2o.dummy.rdd.mul.factor", "2")
  }

  private def testExistenceOfGettersAndSetters(
      traitClass: Class[_],
      objectClass: Class[_],
      gettersAssignedToSameProperty: Seq[Set[String]] = Seq.empty): Int = {
    val methods = traitClass.getDeclaredMethods()
    val propertyMethods = objectClass.getDeclaredMethods().filter { method =>
      method.getName.startsWith("PROP") && method.getReturnType.getSimpleName == "Tuple4"
    }
    val otherMethods = methods.filter(!_.getName.startsWith("PROP"))
    val (setterMethods, getterMethods) = otherMethods
      .filter { method =>
        val modifiers = method.getModifiers()
        !Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers)
      }
      .partition(m => m.getReturnType.getSimpleName == "H2OConf")

    val numberOfProperties = propertyMethods.length
    val gettersWithoutGroupItems = getterMethods.map(_.getName).diff(gettersAssignedToSameProperty.flatten)
    val numberOfGetters = gettersWithoutGroupItems.length + gettersAssignedToSameProperty.length
    val (booleanSetterMethods, otherSetterMethods) = setterMethods.partition(_.getParameterTypes().length == 0)
    val numberOfSetters = booleanSetterMethods.length / 2 + otherSetterMethods.map(_.getName).distinct.length

    booleanSetterMethods.length % 2 shouldEqual 0
    getterMethods.length shouldEqual gettersWithoutGroupItems.length + gettersAssignedToSameProperty.flatten.length
    numberOfProperties shouldEqual numberOfGetters
    numberOfProperties shouldEqual numberOfSetters

    numberOfProperties
  }

  test("Test parity between properties and getters/setters on InternalBackendConf") {
    val numberOfProperties =
      testExistenceOfGettersAndSetters(classOf[InternalBackendConf], InternalBackendConf.getClass)
    numberOfProperties shouldEqual 8
  }

  test("Test parity between properties and getters/setters on ExternalBackendConf") {
    val getterGroups = Seq(
      Set("h2oCluster", "h2oClusterHost", "h2oClusterPort"),
      Set("isAutoClusterStartUsed", "isManualClusterStartUsed", "clusterStartMode"))
    val numberOfProperties =
      testExistenceOfGettersAndSetters(classOf[ExternalBackendConf], ExternalBackendConf.getClass, getterGroups)
    numberOfProperties shouldEqual 30
  }

  test("Test parity between properties and getters/setters on SharedBackendConf") {
    val getterGroups = Seq(Set("backendClusterMode", "runsInExternalClusterMode", "runsInInternalClusterMode"))
    val numberOfProperties =
      testExistenceOfGettersAndSetters(classOf[SharedBackendConf], SharedBackendConf.getClass, getterGroups)
    numberOfProperties shouldEqual 55
  }
}
