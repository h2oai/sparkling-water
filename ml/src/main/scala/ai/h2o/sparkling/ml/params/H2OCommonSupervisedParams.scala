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
package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param._

/**
  * This trait contains parameters that are shared across all supervised algorithms.
  */
trait H2OCommonSupervisedParams extends H2OCommonParams with H2OSupervisedMOJOParams {

  protected final val labelCol = new Param[String](this, "labelCol", "Label column name")

  setDefault(labelCol -> "label")

  def getLabelCol(): String = $(labelCol)

  def setLabelCol(columnName: String): this.type = set(labelCol, columnName)

  def setOffsetCol(columnName: String): this.type = set(offsetCol, columnName)

  override protected def getExcludedCols(): Seq[String] = {
    Seq(getLabelCol(), getFoldCol(), getWeightCol(), getOffsetCol())
      .flatMap(Option(_)) // Remove nulls
  }
}
