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

import ai.h2o.sparkling.backend.exceptions.RestApiUnauthorisedException
import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import java.io.{File, FileWriter}

@RunWith(classOf[JUnitRunner])
class PamAuthentificationTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = {
    val tmpFile = File.createTempFile("sparkling-water-", "-pam-login.conf")
    tmpFile.deleteOnExit()
    val writer = new FileWriter(tmpFile);
    val content =
      """pamloginmodule {
        |     de.codedo.jaas.PamLoginModule required
        |     service = common-auth;
        |};
        |""".stripMargin
    writer.write(content)
    writer.flush()
    writer.close()
    val sparkConf = defaultSparkConf
      .set("spark.ext.h2o.pam.login", "true")
      .set("spark.ext.h2o.login.conf", tmpFile.getAbsolutePath)
    sparkSession("local-cluster[2,1,1024]", sparkConf)
  }

  test("Convert dataframe to h2o frame") {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

    hc.asH2OFrame(df)
  }

  test("Proxy is not accessible with generated internal credentials") {
    // when pam enabled : SW generates credentials for communication with h2o cluster via hash login
    val conf = hc.getConf
    conf.setH2OCluster(hc.flowIp, hc.flowPort)
    intercept[RestApiUnauthorisedException](RestApiUtils.getPingInfo(conf))
  }

  test("Proxy is accessible with correct credentials") {
    val conf = hc.getConf
    conf.setH2OCluster(hc.flowIp, hc.flowPort)
    conf.setUserName("jenkins")
    conf.setPassword("jenkins")
    RestApiUtils.getPingInfo(conf)
  }
}
