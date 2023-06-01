package ai.h2o.sparkling.backend.api.dataframes

import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest

/** Schema representing /3/dataframe/[dataframe_id]/h2oframe endpoint */
case class DataFrameToH2OFrame(dataframe_id: String, h2oframe_id: String)

object DataFrameToH2OFrame extends ParameterBase with DataFrameCommons {
  private[dataframes] case class DataFrameToH2OFrameParameters(dataFrameId: String, h2oFrameId: Option[String]) {
    def validate(): Unit = validateDataFrameId(dataFrameId)
  }

  object DataFrameToH2OFrameParameters {
    private[dataframes] def parse(request: HttpServletRequest): DataFrameToH2OFrameParameters = {
      val dataFrameId = request.getPathInfo.drop(1).split("/").head
      val h2oFrameId = getParameterAsString(request, "h2oframe_id")
      DataFrameToH2OFrameParameters(dataFrameId, Option(h2oFrameId).map(_.toLowerCase()))
    }
  }
}
