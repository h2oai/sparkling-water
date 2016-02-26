package water.api.DataFrames;

import water.api.API;
import water.api.Schema;

/**
 * Schema representing /3/dataframe/&lt;dataframe_id&gt;/h2oframe endpoint
 */
public class H2OFrameIDV3 extends Schema<IcedH2OFrameID, H2OFrameIDV3> {
    @API(help = "ID of Spark's DataFrame to be transformed", direction = API.Direction.INPUT)
    public String dataframe_id;

    @API(help = "ID of generated transformed H2OFrame", direction = API.Direction.INOUT)
    public String h2oframe_id;
}
