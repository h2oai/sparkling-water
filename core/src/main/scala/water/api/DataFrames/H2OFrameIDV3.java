package water.api.DataFrames;

import water.api.API;
import water.api.Schema;

/**
 * Schema used for representing frame name
 */
public class H2OFrameIDV3 extends Schema<IcedH2OFrameID, H2OFrameIDV3> {
    @API(help = "Id of DataFrame to be transformed", direction = API.Direction.INPUT)
    public String dataframe_id;

    @API(help = "Id of transformed H2OFrame", direction = API.Direction.OUTPUT)
    public String h2oframe_id;

    @API(help = "Additional message", direction = API.Direction.OUTPUT)
    public String msg;
}
