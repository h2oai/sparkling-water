package ai.h2o.sparkling.extensions.rest.api.schema;

import water.Iced;
import water.api.API;
import water.api.schemas3.RequestSchemaV3;

public class LogLevelV3 extends RequestSchemaV3<Iced, LogLevelV3> {

  @API(help = "Log Level", direction = API.Direction.INOUT)
  public String log_level = "INFO";
}
