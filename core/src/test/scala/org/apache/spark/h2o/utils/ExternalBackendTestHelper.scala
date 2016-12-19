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

import org.apache.spark.SparkConf
import org.apache.spark.h2o.backends.SharedBackendConf._
import org.scalatest.Suite

import scala.sys.process.Process
import scala.util.Random

/**
  * Used to start H2O nodes from scala code
  */
trait ExternalBackendTestHelper {
  self: Suite =>

  @transient var nodeProcesses: Seq[Process] = _

  lazy val swJar = sys.props.getOrElse("sparkling.assembly.jar", sys.env.getOrElse("sparkling.assembly.jar",
    fail("sparkling.assembly.jar environment variable is not set! It should point to the location of sparkling-water" +
    " assembly JAR")))

  lazy val h2oExtendedJar = sys.props.getOrElse("H2O_EXTENDED_JAR", sys.env.getOrElse("H2O_EXTENDED_JAR",
    fail("H2O_EXTENDED_JAR environment variable is not set! It should point to the location of H2O assembly jar file")))

  lazy val clusterStartTimeout = sys.props.getOrElse("cluster.start.timeout", sys.env.getOrElse("cluster.start.timeout", "6000")).toInt

  def uniqueCloudName(customPart: String) = s"sparkling-water-$customPart-${Random.nextInt()}"

  private def launchSingle(cloudName: String, ip: String, additionalCp: String*): Process = {
    // Since some tests requires additional classes to be present at H2O classpath we add them here
    // instead of extending h2o jar by another classes
    // The best solution would be to implement distributed classloading for H2O
    val jarList =  List(h2oExtendedJar) ++ additionalCp.toList
    val cmdToLaunch = Seq[String]("java", "-cp", jarList.mkString(":"), "water.H2OApp", "-md5skip", "-name", cloudName, "-ip", ip)
    Process(cmdToLaunch).run()
  }

  def runH2OClusterOnYARN() = sys.props.get("spark.ext.h2o.external.start.mode").exists(_ == "auto")
  
  def startCloud(cloudSize: Int, cloudName: String, ip: String, additionalCp: String*): Unit = {
    // do not start h2o nodes if this property is set, they will be started on yarn automatically
    if(!runH2OClusterOnYARN()) {
      nodeProcesses = (1 to cloudSize).map { _ => launchSingle(cloudName, ip, additionalCp: _*) }
      // Wait to ensure that h2o nodes are created earlier than h2o client
      Thread.sleep(clusterStartTimeout)
    }
  }

  def startCloud(cloudSize: Int, sparkConf: SparkConf, additionalCp: String*): Unit = {
    startCloud(cloudSize, sparkConf.get(PROP_CLOUD_NAME._1), sparkConf.get(PROP_CLIENT_IP._1), additionalCp: _*)
  }

  def stopCloud(): Unit = {
    if(!runH2OClusterOnYARN()) {
      if (nodeProcesses != null) {
        nodeProcesses.foreach(_.destroy())
        nodeProcesses = null
      }
    }
  }
}
