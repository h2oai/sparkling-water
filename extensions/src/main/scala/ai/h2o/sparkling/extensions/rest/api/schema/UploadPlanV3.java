/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.h2o.sparkling.extensions.rest.api.schema;

import water.H2ONode;
import water.Iced;
import water.api.API;
import water.api.schemas3.FrameChunksV3;
import water.api.schemas3.RequestSchemaV3;
import water.api.schemas3.SchemaV3;
import water.fvec.Vec;

public class UploadPlanV3 extends RequestSchemaV3<Iced, UploadPlanV3> {

    @API(help = "Required number of chunks", direction = API.Direction.INOUT)
    public int number_of_chunks = -1;

    @API(help = "Column Names", direction = API.Direction.INPUT)
    public ChunkAssigmentV3[] layout = null;

    public static class ChunkAssigmentV3 extends SchemaV3<Iced, UploadPlanV3.ChunkAssigmentV3> {

        @API(help="An identifier unique in scope of a given frame", direction=API.Direction.OUTPUT)
        public int chunk_id;

        @API(help="Index of H2O node where the chunk should be uploaded to", direction=API.Direction.OUTPUT)
        public int node_idx;

        @API(help="IP address of H2O node where the chunk should be uploaded to", direction=API.Direction.OUTPUT)
        public String ip;

        @API(help="Port of H2O node where the chunk should be uploaded to", direction=API.Direction.OUTPUT)
        public int port;

        public ChunkAssigmentV3() {}

        public ChunkAssigmentV3(int id, H2ONode node) {
            this.chunk_id = id;
            this.node_idx = node.index();
            this.ip = node.getIp();
            this.port = new Integer(node.getIpPortString().split(":")[1]);
        }
    }
}
