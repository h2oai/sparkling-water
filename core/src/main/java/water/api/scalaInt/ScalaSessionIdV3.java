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
package water.api.scalaInt;

import water.api.API;
import water.api.Schema;

/**
 * Schema representing [POST] and [DELETE] /3/scalaint endpoint
 */
public class ScalaSessionIdV3  extends Schema<IcedSessionId, ScalaSessionIdV3> {
    @API(help = "Session id identifying the correct interpreter", direction = API.Direction.INOUT)
    public int session_id;
}
