package ai.h2o.sparkling.extensions.rest.api

import ai.h2o.sparkling.extensions.rest.api.schema.SWAvailableV3
import water.api.Handler

final class SparklingWaterAvailableHandler extends Handler {

  def isSWAvailable(version: Int, request: SWAvailableV3): SWAvailableV3 = {
    request
  }
}
