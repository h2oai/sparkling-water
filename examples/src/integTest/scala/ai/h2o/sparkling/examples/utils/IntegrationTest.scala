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

/**
 * Integration test support to be run on top of Spark.
 */
trait IntegrationTest extends FunSuite {

  private def testClassName(obj: Any): String = obj.getClass.getName.replace("$", "")

  /** Launch given class name via SparkSubmit and use given environment
   * to configure SparkSubmit command line.
   */
  def launch(obj: Any, env: IntegrationTestEnv): Unit = {
    val className = testClassName(obj)
    val cmdToLaunch = Seq[String](
      getSubmitScript(env.sparkHome),
      "--class", className,
      "--jars", env.assemblyJar,
      "--verbose",
      "--master", env.sparkMaster) ++
      env.conf.get("spark.driver.memory").map(m => Seq("--driver-memory", m)).getOrElse(Nil) ++
      Seq("--conf", "spark.scheduler.minRegisteredResourcesRatio=1") ++
      Seq("--conf", "spark.ext.h2o.repl.enabled=false") ++ // disable repl in tests
      Seq("--conf", s"spark.test.home=${env.sparkHome}") ++
      Seq("--conf", "spark.task.maxFailures=1") ++ // Any task failures are suspicious
      Seq("--conf", "spark.rpc.numRetries=1") ++ // Any RPC failures are suspicious
      Seq("--conf", "spark.deploy.maxExecutorRetries=1") ++ // Fail directly, do not try to restart executors
      Seq("--conf", "spark.network.timeout=360s") ++ // Increase network timeout if jenkins machines are busy
      Seq("--conf", "spark.worker.timeout=360") ++ // Increase worker timeout if jenkins machines are busy
      // Need to disable timeline service which requires Jersey libraries v1, but which are not available in Spark2.0
      // See: https://www.hackingnote.com/en/spark/trouble-shooting/NoClassDefFoundError-ClientConfig/
      Seq("--conf", "spark.hadoop.yarn.timeline-service.enabled=false") ++
      Seq("--conf", s"spark.ext.h2o.external.start.mode=auto") ++
      Seq("--conf", s"spark.ext.h2o.backend.cluster.mode=${sys.props.getOrElse("spark.ext.h2o.backend.cluster.mode", "internal")}") ++
      Seq("--conf", s"spark.ext.h2o.external.disable.version.check=true") ++
      env.conf.flatMap(p => Seq("--conf", s"${p._1}=${p._2}")) ++
      Seq[String](env.itestJar)

    import scala.sys.process._
    val proc = cmdToLaunch.!
    assert(proc == 0, s"Process finished in wrong way! response=$proc from \n${cmdToLaunch mkString " "}")
  }

  /** Determines whether we run on Windows or Unix and return correct spark-submit script location */
  private def getSubmitScript(sparkHome: String): String = {
    if (System.getProperty("os.name").startsWith("Windows")) {
      sparkHome + "\\bin\\spark-submit.cmd"
    } else {
      sparkHome + "/bin/spark-submit"
    }
  }
}
