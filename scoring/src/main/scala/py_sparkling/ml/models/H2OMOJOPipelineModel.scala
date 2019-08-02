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

package py_sparkling.ml.models

import ai.h2o.sparkling.ml.models.{H2OMOJOLoader, H2OMOJOReadable}
import org.apache.spark.ml.h2o.models.H2OMOJOSettings

class H2OMOJOPipelineModel(override val uid: String) extends org.apache.spark.ml.h2o.models.H2OMOJOPipelineModel(uid)

object H2OMOJOPipelineModel extends H2OMOJOReadable[H2OMOJOPipelineModel] with H2OMOJOLoader[H2OMOJOPipelineModel] {
  override def createFromMojo(mojoData: Array[Byte], uid: String, settings: H2OMOJOSettings): H2OMOJOPipelineModel = {
    org.apache.spark.ml.h2o.models.H2OMOJOPipelineModel.createFromMojo(mojoData, uid, settings)
  }
}
