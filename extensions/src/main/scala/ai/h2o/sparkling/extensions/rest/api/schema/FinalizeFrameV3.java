package ai.h2o.sparkling.extensions.rest.api.schema;

import water.Iced;
import water.api.API;
import water.api.schemas3.RequestSchemaV3;

public class FinalizeFrameV3 extends RequestSchemaV3<Iced, FinalizeFrameV3> {

  @API(help = "Frame name", direction = API.Direction.INPUT)
  public String key = null;

  @API(
      help =
          "Number of rows represented by individual chunks. The type is long[] encoded with base64 encoding.",
      direction = API.Direction.INPUT)
  public String rows_per_chunk = null;

  @API(
      help = "H2O types of individual columns. The type is byte[] encoded with base64 encoding.",
      direction = API.Direction.INPUT)
  public String column_types = null;
}
