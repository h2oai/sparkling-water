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

package org.apache.spark.ml.h2o.models

import java.io.ByteArrayInputStream

import ai.h2o.mojos.runtime.MojoPipeline
import ai.h2o.mojos.runtime.readers.MojoPipelineReaderBackendFactory
import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.ml.models.H2OMOJOReadable

@Deprecated
class H2OMOJOPipelineModel(override val uid: String) extends ai.h2o.sparkling.ml.models.H2OMOJOPipelineModel(uid)

@Deprecated
object H2OMOJOPipelineModel extends H2OMOJOReadable[ai.h2o.sparkling.ml.models.H2OMOJOPipelineModel] with H2OMOJOLoader[H2OMOJOPipelineModel] {

  @DeprecatedMethod("ai.h2o.sparkling.ml.models.H2OMOJOPipelineModel.createFromMojo")
  override def createFromMojo(mojoData: Array[Byte], uid: String, settings: H2OMOJOSettings): H2OMOJOPipelineModel = {
    val model = new H2OMOJOPipelineModel(uid)
    val reader = MojoPipelineReaderBackendFactory.createReaderBackend(new ByteArrayInputStream(mojoData))
    val featureCols = MojoPipeline.loadFrom(reader).getInputMeta.getColumnNames
    model.set(model.featuresCols, featureCols)
    model.set(model.outputCols, MojoPipeline.loadFrom(reader).getOutputMeta.getColumnNames)
    model.set(model.convertUnknownCategoricalLevelsToNa -> settings.convertUnknownCategoricalLevelsToNa)
    model.set(model.convertInvalidNumbersToNa -> settings.convertInvalidNumbersToNa)
    model.set(model.namedMojoOutputColumns -> settings.namedMojoOutputColumns)
    model.setMojoData(mojoData)
    model
  }
}
