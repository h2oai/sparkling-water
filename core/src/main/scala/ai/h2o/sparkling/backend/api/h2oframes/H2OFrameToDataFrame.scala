package ai.h2o.sparkling.backend.api.h2oframes

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest
import water.exceptions.H2ONotFoundArgumentException

/** Schema representing /3/h2oframes/[h2oframe_id]/dataframe */
case class H2OFrameToDataFrame(h2oframe_id: String, dataframe_id: String)

object H2OFrameToDataFrame extends ParameterBase {
  private[h2oframes] case class H2OFrameToDataFrameParameters(h2oFrameId: String, dataframeId: Option[String]) {
    def validate(): Unit = {
      if (!H2OFrame.exists(h2oFrameId)) {
        throw new H2ONotFoundArgumentException(
          s"H2OFrame with id '$h2oFrameId' does not exist, can not proceed with the transformation!")
      }
    }
  }

  private[h2oframes] object H2OFrameToDataFrameParameters {
    def parse(request: HttpServletRequest): H2OFrameToDataFrameParameters = {
      val h2oFrameId = request.getPathInfo.drop(1).split("/").head
      val dataFrameId = getParameterAsString(request, "dataframe_id")
      H2OFrameToDataFrameParameters(h2oFrameId, Option(dataFrameId).map(_.toLowerCase()))
    }
  }
}
