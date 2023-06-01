package ai.h2o.sparkling.backend

import ai.h2o.sparkling.backend.utils.ProgressBar.printProgressBar
import ai.h2o.sparkling.backend.utils.RestApiUtils.getClusterEndpoint
import ai.h2o.sparkling.backend.utils.RestCommunication
import ai.h2o.sparkling.{H2OConf, H2OContext}
import org.apache.spark.expose.Logging
import water.api.schemas3.{JobV3, JobsV3}

private[sparkling] class H2OJob private (val id: String) extends Logging {
  private val conf = H2OContext.ensure("H2OContext needs to be running!").getConf

  def waitForFinishAndPrintProgress(): Unit = {
    while (true) {
      val job = H2OJob.verifyAndGetJob(conf, id)
      val status = H2OJobStatus.fromString(job.status)
      status match {
        case H2OJobStatus.DONE =>
          if (conf.isProgressBarEnabled) printProgressBar(job.progress, leaveTheProgressBarVisible = true)
          logInfo(s"H2O Job $id finished successfully.")
          return
        case H2OJobStatus.FAILED => throw new Exception(s"""H2O Job $id has failed!
               |Exception: ${job.exception}
               |StackTrace: ${job.stacktrace}""".stripMargin)
        case H2OJobStatus.CANCELLED => throw new Exception(s"H2O Job $id has been cancelled!")
        case H2OJobStatus.RUNNING =>
          if (conf.isProgressBarEnabled) printProgressBar(job.progress)
          val progressPercent = (job.progress * 100).ceil.toInt
          logInfo(s"Waiting for job $id to finish... $progressPercent%")
          Thread.sleep(500)
        case _ => throw new RuntimeException(s"Job state '$status' is not handled at this moment.")
      }
    }
  }
}

private[sparkling] object H2OJob extends RestCommunication {
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
