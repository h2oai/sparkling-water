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
 * Schema representing /3/dataframes/[dataframe_id] endpoint
 */
public class DataFrameV3 extends Schema<IcedDataFrame, DataFrameV3> {
    @API(help = "Data frame ID", direction = API.Direction.INOUT)
    public String dataframe_id;

    @API(help = "Number of partitions", direction = API.Direction.OUTPUT)
    public int partitions;

    @API(help = "Schema of this DataFrame.", direction = API.Direction.OUTPUT)
    public String schema;
}
