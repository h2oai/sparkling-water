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
package ai.h2o.sparkling.backend.api.dataframes;

import water.api.API;
import water.api.Schema;

/**
 * Schema representing /3/dataframe/[dataframe_id]/h2oframe endpoint
 */
public class H2OFrameIDV3 extends Schema<IcedH2OFrameID, H2OFrameIDV3> {
    @API(help = "ID of Spark's DataFrame to be transformed", direction = API.Direction.INPUT)
    public String dataframe_id;

    @API(help = "ID of generated transformed H2OFrame", direction = API.Direction.INOUT)
    public String h2oframe_id;
}
