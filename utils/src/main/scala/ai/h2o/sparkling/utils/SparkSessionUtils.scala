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

package ai.h2o.sparkling.utils

import java.io.{File, InputStream}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.expose.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.{ExposeUtils, SparkConf, expose}

/**
  * Internal utilities methods for Spark Session
  */
object SparkSessionUtils extends Logging {
  def active: SparkSession = {
    SparkSession.builder().getOrCreate()
  }

  def createSparkSession(conf: SparkConf, forceHive: Boolean = false): SparkSession = {
    val builder = SparkSession.builder.config(conf)
    if (conf.get(CATALOG_IMPLEMENTATION.key, "hive").toLowerCase == "hive" || forceHive) {
      if (ExposeUtils.hiveClassesArePresent || forceHive) {
        builder.enableHiveSupport()
        logInfo("Enabling Hive support for Spark session")
      } else {
        builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
      }
    } else {
      // Nop
    }
    val sparkSession = builder.getOrCreate()
    logInfo("Created Spark session")
    sparkSession
  }

  def readHDFSFile(path: String): InputStream = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(SparkSessionUtils.active.sparkContext.hadoopConfiguration)
    val qualifiedPath = hadoopPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    fs.open(qualifiedPath)
  }

  def hdfsQualifiedPath(path: String): String = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(SparkSessionUtils.active.sparkContext.hadoopConfiguration)
    hadoopPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
  }

  def inputStreamToTempFile(inputStream: InputStream, filePrefix: String, fileSuffix: String): File = {
    val sparkSession = SparkSessionUtils.active
    val sparkTmpDir = expose.Utils.createTempDir(expose.Utils.getLocalDir(sparkSession.sparkContext.getConf))
    val outputFile = File.createTempFile(filePrefix, fileSuffix, sparkTmpDir)
    FileUtils.copyInputStreamToFile(inputStream, outputFile)
    outputFile
  }
}
