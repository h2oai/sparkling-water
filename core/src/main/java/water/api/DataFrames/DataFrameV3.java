package water.api.DataFrames;


import water.api.API;
import water.api.Schema;

/**
 * Schema representing /3/dataframes/&lt;dataframe_id&gt; endpoint
 */
public class DataFrameV3 extends Schema<IcedDataFrameInfo, DataFrameV3> {
    @API(help = "Data frame ID", direction = API.Direction.INOUT)
    public String dataframe_id;

    @API(help = "Number of partitions", direction = API.Direction.OUTPUT)
    public int partitions;

    @API(help = "Schema of this DataFrame.", direction = API.Direction.OUTPUT)
    public String schema;
}
