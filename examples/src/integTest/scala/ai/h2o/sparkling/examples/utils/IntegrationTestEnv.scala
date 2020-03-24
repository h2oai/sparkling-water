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

package ai.h2o.sparkling.examples.utils

import org.scalatest.FunSuite
import water.init.NetworkInit

trait IntegrationTestEnv extends FunSuite {
  lazy val sparkHome: String = sys.props.getOrElse("SPARK_HOME",
    sys.props.getOrElse("spark.test.home",
      fail("None of both 'SPARK_HOME' and 'spark.test.home' variables is not set! It should point to Spark home directory.")))

  lazy val assemblyJar: String = sys.props.getOrElse("sparkling.assembly.jar",
    fail("The variable 'sparkling.assembly.jar' is not set! It should point to assembly jar file."))

  lazy val itestJar: String = sys.props.getOrElse("sparkling.itest.jar",
    fail("The variable 'sparkling.itest.jar' should point to a jar containing integration test classes!"))

  val sparkMaster: String

  def conf: Map[String, String] = Map(
    "spark.testing" -> "true",
    "spark.ext.h2o.client.ip" -> sys.props.getOrElse("H2O_CLIENT_IP", NetworkInit.findInetAddressForSelf().getHostAddress))
}
