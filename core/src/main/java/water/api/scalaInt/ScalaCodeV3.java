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
 * Schema representing [POST] /3/scalaint/&lt;session_id&gt; endpoint.
 */
public class ScalaCodeV3 extends Schema<IcedCode, ScalaCodeV3> {

  @API(help = "Session id identifying the correct interpreter", direction = API.Direction.INPUT)
  public int session_id;

  @API(help = "Scala code to interpret", direction = API.Direction.INPUT)
  public String code;

  @API(help = "Status of the code execution", direction = API.Direction.OUTPUT)
  public String status;

  @API(help = "Response of the interpreter", direction = API.Direction.OUTPUT)
  public String response;

  @API(help = "Redirected console output, for example output of println is stored to this field",
      direction = API.Direction.OUTPUT)
  public String output;
}

