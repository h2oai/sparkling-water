package ai.h2o.sparkling.backend.api.dataframes

import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest

/** Schema representing /3/dataframes/[dataframe_id] endpoint */
case class DataFrameInfo(dataframe_id: String, partitions: Int, schema: String)

object DataFrameInfo extends ParameterBase with DataFrameCommons {

  private[dataframes] case class DataFrameInfoParameters(dataFrameId: String) {
    def validate(): Unit = validateDataFrameId(dataFrameId)
  }

  private[dataframes] object DataFrameInfoParameters {
    def parse(request: HttpServletRequest): DataFrameInfoParameters = {
      val dataFrameId = request.getPathInfo.drop(1).split("/").head
      DataFrameInfoParameters(dataFrameId)
    }
  }
}
