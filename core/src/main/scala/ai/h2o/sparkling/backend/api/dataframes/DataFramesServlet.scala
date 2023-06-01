package ai.h2o.sparkling.backend.api.dataframes

import ai.h2o.sparkling.backend.api.{ServletBase, ServletRegister}
import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.{H2OConf, H2OContext}
import javax.servlet.Servlet
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import water.exceptions.H2ONotFoundArgumentException

/**
  * Handler for all Spark's DataFrame related queries
  */
private[api] class DataFramesServlet extends ServletBase {
  private lazy val sqlContext = SparkSessionUtils.active.sqlContext

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case null => list()
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)
  }

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case s if s.matches("/.*/h2oframe") =>
        val parameters = DataFrameToH2OFrame.DataFrameToH2OFrameParameters.parse(req)
        parameters.validate()
        toH2OFrame(parameters.dataFrameId, parameters.h2oFrameId)
      case s if s.matches("/.*") =>
        val parameters = DataFrameInfo.DataFrameInfoParameters.parse(req)
        parameters.validate()
        getDataFrame(parameters.dataFrameId)
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)

  }

  def list(): DataFrames = {
    DataFrames(fetchAll())
  }

  def fetchAll(): Array[DataFrameInfo] = {
    val names = sqlContext.tableNames()
    names.map(name => getDataFrame(name))
  }

  def getDataFrame(dataFrameId: String): DataFrameInfo = {
    val dataFrame = sqlContext.table(dataFrameId)
    DataFrameInfo(dataFrameId, dataFrame.rdd.partitions.length, dataFrame.schema.json)
  }

  def toH2OFrame(dataFrameId: String, h2oFrameId: Option[String]): DataFrameToH2OFrame = {
    val dataFrame = sqlContext.table(dataFrameId)
    val h2oFrame = H2OContext.ensure().asH2OFrame(dataFrame, h2oFrameId)
    DataFrameToH2OFrame(dataFrameId, h2oFrame.frameId)
  }

}

object DataFramesServlet extends ServletRegister {

  override protected def getRequestPaths(): Array[String] = Array("/3/dataframes", "/3/dataframes/*")

  override protected def getServlet(conf: H2OConf, hc: H2OContext): Servlet = new DataFramesServlet
}
