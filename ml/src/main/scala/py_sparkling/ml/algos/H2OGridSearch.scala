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

package py_sparkling.ml.algos

import hex.grid.Grid
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.algos.{H2OGridSearchParams, H2OGridSearchReader}
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.SQLContext
import py_sparkling.ml.models.H2OMOJOModel
import water.support.ModelSerializationSupport

/**
  * H2O Grid Search Wrapper for PySparkling
  */
class H2OGridSearch(override val gridSearchParams: Option[H2OGridSearchParams], override val uid: String)
               (implicit hc: H2OContext, sqlContext: SQLContext)
  extends org.apache.spark.ml.h2o.algos.H2OGridSearch {

  def this()(implicit hc: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("grid_search"))

  def this(uid: String, hc: H2OContext, sqlContext: SQLContext) = this(None, uid)(hc, sqlContext)

  override def trainModel(grid: Grid[_]) = {
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(selectModelFromGrid(grid)))
  }
}


object H2OGridSearch extends MLReadable[H2OGridSearch] {

  private final val defaultFileName = org.apache.spark.ml.h2o.algos.H2OGridSearch.defaultFileName

  override def read: MLReader[H2OGridSearch] = H2OGridSearchReader.create[H2OGridSearch](defaultFileName)

  override def load(path: String): H2OGridSearch = super.load(path)
}
