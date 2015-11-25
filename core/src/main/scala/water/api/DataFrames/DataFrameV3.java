package water.api.DataFrames;


import water.api.API;
import water.api.Schema;

/**
 * Endpoint representing single DataFrame.
 */
public class DataFrameV3 extends Schema<IcedDataFrameInfo, DataFrameV3> {
    @API(help = "Data frame id", direction = API.Direction.INOUT)
    public String dataframe_id;

    @API(help = "Schema of this DataFrame.", direction = API.Direction.OUTPUT)
    public String schema;
}
