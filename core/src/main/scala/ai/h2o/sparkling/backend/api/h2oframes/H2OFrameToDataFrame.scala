package ai.h2o.sparkling.backend.api.h2oframes

import water.api.API

/** Schema representing /3/h2oframes/[h2oframe_id]/dataframe */
class H2OFrameToDataFrame(
    @API(help = "ID of H2OFrame to be transformed", direction = API.Direction.INPUT)
    var h2oframe_id: String,
    @API(help = "ID of generated Spark's DataFrame", direction = API.Direction.INOUT)
    var dataframe_id: String)
