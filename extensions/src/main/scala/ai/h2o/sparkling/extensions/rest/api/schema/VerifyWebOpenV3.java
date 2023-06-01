package ai.h2o.sparkling.extensions.rest.api.schema;

import water.Iced;
import water.api.API;
import water.api.schemas3.RequestSchemaV3;

public class VerifyWebOpenV3 extends RequestSchemaV3<Iced, VerifyWebOpenV3> {

  @API(help = "Nodes with disabled web", direction = API.Direction.OUTPUT)
  public String[] nodes_web_disabled;
}
