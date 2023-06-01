package ai.h2o.sparkling.backend.api.rdds

import java.net.URI

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.backend.utils.RestApiUtils

trait RDDsRestApi extends RestApiUtils {

  protected def listRDDs(): RDDs = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    query[RDDs](endpoint, "/3/RDDs", hc.getConf)
  }

  protected def getRDD(rddId: Int): RDDInfo = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    update[RDDInfo](endpoint, s"/3/RDDs/$rddId", hc.getConf)
  }

  protected def convertToH2OFrame(rddId: Int, h2oFrameId: String): RDDToH2OFrame = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    val params = Map("h2oframe_id" -> h2oFrameId)
    update[RDDToH2OFrame](endpoint, s"/3/RDDs/$rddId/h2oframe", hc.getConf, params)
  }
}
