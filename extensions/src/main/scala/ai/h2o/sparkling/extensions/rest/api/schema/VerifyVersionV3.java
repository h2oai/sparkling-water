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
