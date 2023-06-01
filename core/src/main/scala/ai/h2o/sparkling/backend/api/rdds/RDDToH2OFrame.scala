package ai.h2o.sparkling.backend.api.rdds

import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest

/** Schema representing /3/RDDs/[rdd_id]/h2oframe endpoint */
case class RDDToH2OFrame(rdd_id: Int, h2oframe_id: String)

object RDDToH2OFrame extends ParameterBase with RDDCommons {
  private[rdds] case class RDDToH2OFrameParameters(rddId: Int, h2oFrameId: Option[String]) {
    def validate(): Unit = validateRDDId(rddId)
  }

  object RDDToH2OFrameParameters {
    private[rdds] def parse(request: HttpServletRequest): RDDToH2OFrameParameters = {
      val rddId = request.getPathInfo.drop(1).split("/").head.toInt
      val h2oFrameId = getParameterAsString(request, "h2oframe_id")
      RDDToH2OFrameParameters(rddId, Option(h2oFrameId).map(_.toLowerCase()))
    }
  }
}
