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

package ai.h2o.sparkling.backend.utils

import java.io.File

import org.apache.spark.h2o.H2OContext
import water.api.ImportHiveTableHandler
import water.api.ImportHiveTableHandler.HiveTableImporter
import water.fvec.Frame

trait H2OContextExtensions extends H2OContextUtils {
  _: H2OContext =>
  def downloadH2OLogs(destinationDir: String, logContainer: String): String = {
    verifyLogContainer(logContainer)
    val endpoint = RestApiUtils.getClusterEndpoint(getConf)
    val file = new File(destinationDir, s"${logFileName()}.${logContainer.toLowerCase}")
    val logEndpoint = s"/3/Logs/download/$logContainer"
    logContainer match {
      case "LOG" =>
        downloadStringURLContent(endpoint, logEndpoint, getConf, file)
      case "ZIP" =>
        downloadBinaryURLContent(endpoint, logEndpoint, getConf, file)
    }
    file.getAbsolutePath
  }

  def importHiveTable(database: String = HiveTableImporter.DEFAULT_DATABASE, table: String,
                      partitions: Array[Array[String]] = null, allowMultiFormat: Boolean = false): Frame = {
    val hiveTableHandler = new ImportHiveTableHandler
    val method = hiveTableHandler.getClass.getDeclaredMethod("getImporter")
    method.setAccessible(true)
    val importer = method.invoke(hiveTableHandler).asInstanceOf[ImportHiveTableHandler.HiveTableImporter]

    if (importer != null) {
      try {
        importer.loadHiveTable(database, table, partitions, allowMultiFormat).get()
      }
      catch {
        case e: NoClassDefFoundError =>
          throw new IllegalStateException("Hive Metastore client classes not available on classpath.", e)
      }
    } else {
      throw new IllegalStateException("HiveTableImporter extension not enabled.")
    }
  }
}
