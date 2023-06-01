package ai.h2o.sparkling.backend.api.h2oframes

import java.net.URI

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestEncodingUtils}

trait H2OFramesRestApi extends RestApiUtils with RestEncodingUtils {

  protected def convertToDataFrame(h2oFrameId: String, dataFrameId: String): H2OFrameToDataFrame = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    val params = if (dataFrameId == null) Map[String, String]() else Map("dataframe_id" -> dataFrameId)
    update[H2OFrameToDataFrame](endpoint, s"/3/h2oframes/$h2oFrameId/dataframe", hc.getConf, params)
  }
}
