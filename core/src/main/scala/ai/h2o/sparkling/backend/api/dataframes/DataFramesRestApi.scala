package ai.h2o.sparkling.backend.api.dataframes

import java.net.URI

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.backend.utils.RestApiUtils

trait DataFramesRestApi extends RestApiUtils {

  protected def listDataFrames(): DataFrames = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    query[DataFrames](endpoint, "/3/dataframes", hc.getConf)
  }

  protected def getDataFrame(dataFrameId: String): DataFrameInfo = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    update[DataFrameInfo](endpoint, s"/3/dataframes/$dataFrameId", hc.getConf)
  }

  protected def convertToH2OFrame(dataFrameId: String, h2oFrameId: String): DataFrameToH2OFrame = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    val params = if (h2oFrameId == null) Map[String, String]() else Map("h2oframe_id" -> h2oFrameId)
    update[DataFrameToH2OFrame](endpoint, s"/3/dataframes/$dataFrameId/h2oframe", hc.getConf, params)
  }
}
