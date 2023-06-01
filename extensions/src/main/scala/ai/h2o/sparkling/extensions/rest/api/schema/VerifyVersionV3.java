package ai.h2o.sparkling.extensions.rest.api.schema;

import water.Iced;
import water.api.API;
import water.api.schemas3.RequestSchemaV3;

public class VerifyVersionV3 extends RequestSchemaV3<Iced, VerifyVersionV3> {

  @API(help = "Reference version to validate against", direction = API.Direction.INPUT)
  public String referenced_version;

  @API(help = "Nodes with wrong versions", direction = API.Direction.OUTPUT)
  public NodeWithVersionV3[] nodes_wrong_version;

  public static class NodeWithVersionV3 extends RequestSchemaV3<Iced, NodeWithVersionV3> {
    @API(help = "Node address", direction = API.Direction.OUTPUT)
    public String ip_port;

    @API(help = "Node version", direction = API.Direction.OUTPUT)
    public String version;
  }
}
