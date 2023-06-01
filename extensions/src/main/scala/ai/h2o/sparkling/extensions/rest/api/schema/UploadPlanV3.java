package ai.h2o.sparkling.extensions.rest.api.schema;

import water.H2ONode;
import water.Iced;
import water.api.API;
import water.api.schemas3.RequestSchemaV3;
import water.api.schemas3.SchemaV3;

public class UploadPlanV3 extends RequestSchemaV3<Iced, UploadPlanV3> {

  @API(help = "Required number of chunks", direction = API.Direction.INPUT)
  public int number_of_chunks = -1;

  @API(help = "Column Names", direction = API.Direction.OUTPUT)
  public ChunkAssigmentV3[] layout = null;

  public static class ChunkAssigmentV3 extends SchemaV3<Iced, UploadPlanV3.ChunkAssigmentV3> {

    @API(help = "An identifier unique in scope of a given frame", direction = API.Direction.OUTPUT)
    public int chunk_id;

    @API(
        help = "Index of H2O node where the chunk should be uploaded to",
        direction = API.Direction.OUTPUT)
    public int node_idx;

    @API(
        help = "IP address of H2O node where the chunk should be uploaded to",
        direction = API.Direction.OUTPUT)
    public String ip;

    @API(
        help = "Port of H2O node where the chunk should be uploaded to",
        direction = API.Direction.OUTPUT)
    public int port;

    public ChunkAssigmentV3() {}

    public ChunkAssigmentV3(int id, H2ONode node) {
      this.chunk_id = id;
      this.node_idx = node.index();
      String[] ipPortArray = node.getIpPortString().split(":");
      this.ip = ipPortArray[0];
      this.port = new Integer(ipPortArray[1]);
    }
  }
}
