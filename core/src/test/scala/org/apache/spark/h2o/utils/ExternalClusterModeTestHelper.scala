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
import org.apache.spark.h2o.backends.SharedH2OConf._
import org.scalatest.Suite

import scala.sys.process.Process
import scala.util.Random

/**
  * Used to start H2O nodes from scala code
  */
trait ExternalClusterModeTestHelper {
  self: Suite =>

  @transient var nodeProcesses: Seq[Process] = _

  lazy val swJar = sys.props.getOrElse("sparkling.assembly.jar", if(sys.env.get("sparkling.assembly.jar").isDefined){
    sys.env("sparkling.assembly.jar")
  } else{
    fail("sparkling.assembly.jar environment variable is not set! It should point to the location of sparkling-water" +
      " assembly JAR")
  })

  lazy val h2oJar = sys.props.getOrElse("H2O_JAR", if (sys.env.get("H2O_JAR").isDefined) {
    sys.env("H2O_JAR")
  } else {
    fail("H2O_JAR environment variable is not set! It should point to the location of H2O assembly jar file")
  })

  def uniqueCloudName(customPart: String) = s"sparkling-water-$customPart-${Random.nextInt()}"

  private def launchSingle(cloudName: String, ip: String): Process = {
    val cmdToLaunch = Seq[String]("java", "-jar", h2oJar, "-md5skip", "-name", cloudName, "-ip", ip)
    import scala.sys.process._
    Process(cmdToLaunch).run()
  }

  def startCloud(cloudSize: Int, cloudName: String, ip: String): Unit = {
    nodeProcesses = (1 to cloudSize).map { _ => launchSingle(cloudName, ip) }
    // Wait 2 seconds to ensure that h2o nodes are created earlier than h2o client
    Thread.sleep(2000)
  }

  def startCloud(cloudSize: Int, sparkConf: SparkConf): Unit = {
    startCloud(cloudSize, sparkConf.get(PROP_CLOUD_NAME._1), sparkConf.get(PROP_CLIENT_IP._1))
  }

  def testsInExternalMode(conf: Option[SparkConf] = None): Boolean = {
    if(conf.isDefined){
      conf.get.getOption(PROP_BACKEND_CLUSTER_MODE._1).getOrElse(PROP_BACKEND_CLUSTER_MODE._2) == "external"
    }else{
      sys.props.getOrElse(PROP_BACKEND_CLUSTER_MODE._1, PROP_BACKEND_CLUSTER_MODE._2) == "external"
    }
  }

  def testsInExternalMode(conf: SparkConf): Boolean = {
    testsInExternalMode(Option(conf))
  }

  def stopCloud(): Unit = {
    if (nodeProcesses != null) {
      nodeProcesses.foreach(_.destroy())
      nodeProcesses = null
    }
  }
}
