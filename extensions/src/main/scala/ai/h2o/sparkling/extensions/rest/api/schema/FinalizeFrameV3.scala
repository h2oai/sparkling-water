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

package ai.h2o.sparkling.extensions.rest.api.schema

import water.Iced
import water.api.API
import water.api.schemas3.RequestSchemaV3

class FinalizeFrameV3 extends RequestSchemaV3[Iced, FinalizeFrameV3] {

  @API(help = "Frame name", direction = API.Direction.INPUT)
  var key: String = ""

  @API(help = "Number of rows represented by individual chunks", direction = API.Direction.INPUT)
  var rows_per_chunk: Array[Long] = null

  @API(help = "H2O types of individual columns", direction = API.Direction.INPUT)
  var column_types: Array[Byte] = null
}
