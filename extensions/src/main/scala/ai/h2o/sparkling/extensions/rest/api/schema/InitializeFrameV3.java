package ai.h2o.sparkling.extensions.rest.api.schema;

import water.Iced;
import water.api.API;
import water.api.schemas3.RequestSchemaV3;

public class InitializeFrameV3 extends RequestSchemaV3<Iced, InitializeFrameV3> {

    @API(help = "Frame Name", direction = API.Direction.INPUT)
    public String key = null;

    @API(help = "Column Names", direction = API.Direction.INPUT)
    public String[] columns = null;
}
