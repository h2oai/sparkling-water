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

package ai.h2o.sparkling.job

import ai.h2o.sparkling.backend.utils.RestApiUtils.getClusterEndpoint
import ai.h2o.sparkling.backend.utils.RestCommunication
import org.apache.spark.expose.Logging
import org.apache.spark.h2o.{H2OConf, H2OContext}
import water.api.schemas3.{JobV3, JobsV3}

class H2OJob private(val id: String) extends Logging {
  private val conf = H2OContext.ensure("H2OContext needs to be running!").getConf

  private def fetchStatus(): H2OJobStatus.Value = {
    H2OJobStatus.fromString(H2OJob.verifyAndGetJob(conf, id).status)
  }

  def waitForFinish(): Unit = {
    while (true) {
      val status = fetchStatus()
      status match {
        case H2OJobStatus.DONE =>
          logInfo(s"H2O Job $id finished successfully.")
          return
        case H2OJobStatus.FAILED => throw new Exception(s"H2O Job $id has failed!")
        case H2OJobStatus.CANCELLED => throw new Exception(s"H2O Job $id has been cancelled!")
        case H2OJobStatus.RUNNING =>
          logInfo(s"Waiting for job $id to finish...")
          Thread.sleep(1000)
        case _ => throw new RuntimeException(s"Job state '$status' is not handled at this moment.")
      }
    }
  }
}

object H2OJob extends RestCommunication {
  def apply(jobId: String): H2OJob = {
    val conf = H2OContext.ensure().getConf
    verifyAndGetJob(conf, jobId)
    new H2OJob(jobId)
  }

  private def verifyAndGetJob(conf: H2OConf, jobId: String): JobV3 = {
    val endpoint = getClusterEndpoint(conf)
    val jobs = query[JobsV3](endpoint, s"/3/Jobs/$jobId", conf)
    if (jobs.jobs.length == 0) {
      throw new IllegalArgumentException(s"Job $jobId does not exist!")
    }
    jobs.jobs(0)
  }
}
