package water.api.RDDs;

import water.api.API;
import water.api.DataFrames.IcedH2OFrameID;
import water.api.Schema;

/**
 * Schema representing /3/RDDs/&lt;rdd_id&gt;/h2oframe endpoint
 */
public class RDD2H2OFrameIDV3 extends Schema<IcedRDD2H2OFrameID, RDD2H2OFrameIDV3> {
    @API(help = "Id of RDD to be transformed", direction = API.Direction.INPUT)
    public int rdd_id;

    @API(help = "Id of transformed H2OFrame", direction = API.Direction.INOUT)
    public String h2oframe_id;

}
