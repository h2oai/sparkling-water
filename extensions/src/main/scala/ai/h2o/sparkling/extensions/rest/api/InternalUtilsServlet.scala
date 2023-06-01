package ai.h2o.sparkling.extensions.rest.api

import ai.h2o.sparkling.extensions.rest.api.schema.ScalaCodeResult
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import com.google.gson.Gson
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import water.H2O.H2OCountedCompleter
import water.api.schemas3.JobV3
import water.server.ServletUtils
import water.{Job, Key}

import scala.collection.concurrent.TrieMap

/**
  * This servlet class handles POST requests for the path /3/SparklingInternal
  */
class InternalUtilsServlet extends ServletBase {

  private val jobs = new TrieMap[String, Boolean]

  case class IdParameter(id: String)

  object IdParameter {
    def parse(request: HttpServletRequest): IdParameter = {
      val jobId = request.getRequestURI.split("/")(4)
      IdParameter(jobId)
    }
  }

  override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    processRequest(request, response) {
      val obj = request.getRequestURI match {
        case s if s.startsWith("/3/SparklingInternal/start/") =>
          val parameters = IdParameter.parse(request)
          submitScalaCodeWrapperJob(parameters.id)
        case s if s.startsWith("/3/SparklingInternal/stop/") =>
          val parameters = IdParameter.parse(request)
          stopScalaCodeWrapperJob(parameters.id)
      }
      sendResult(obj, response)
    }
  }

  private def submitScalaCodeWrapperJob(resultKey: String): JobV3 = {
    val key = Key.make[ScalaCodeResult](resultKey)
    val className = classOf[ScalaCodeResult].getName
    val job = new Job[ScalaCodeResult](key, className, "ScalaCodeResult")
    val jobKey = job._key.toString
    jobs.put(jobKey, true)
    job.start(new H2OCountedCompleter() {
      override def compute2(): Unit = {
        while (jobs.contains(jobKey) && jobs(jobKey)) {
          Thread.sleep(100)
        }
        tryComplete()
      }
    }, 1)
    val jobV3 = new JobV3(job)
    jobs.put(jobKey, true)
    jobV3
  }

  private def stopScalaCodeWrapperJob(id: String): Unit = {
    jobs.put(id, false)
    jobs.remove(id)
  }

  private def sendResult(obj: Any, response: HttpServletResponse): Unit = {
    val json = new Gson().toJson(obj)
    withResource(response.getWriter) { writer =>
      response.setContentType("application/json")
      response.setCharacterEncoding("UTF-8")
      writer.print(json)
    }
    response.setStatus(HttpServletResponse.SC_OK)
    ServletUtils.setResponseStatus(response, HttpServletResponse.SC_OK)
  }
}
