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
package org.apache.spark.ml.h2o

import org.apache.spark.h2o.H2OContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext

package object algos extends Logging{

  implicit class H2OGBMWithImplicits(algo: H2OGBM) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2OGBM = {
      logWarning("The constructor 'new H2OGBM()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'" +
        " is deprecated. Use 'new H2OGBM()'!")
      algo
    }
  }

  implicit class H2OGLMWithImplicits(algo: H2OGLM) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2OGLM = {
      logWarning("The constructor 'new H2OGLM()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'" +
        " is deprecated. Use 'new H2OGLM()'!")
      algo
    }
  }

  implicit class H2ODeepLearningWithImplicits(algo: H2ODeepLearning) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2ODeepLearning = {
      logWarning("The constructor 'new H2ODeepLearning()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'" +
        " is deprecated. Use 'new H2ODeepLearning()'!")
      algo
    }
  }

  implicit class H2OXGboostWithImplicits(algo: H2OXGBoost) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2OXGBoost = {
      logWarning("The constructor 'new H2OXGBoost()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'" +
        " is deprecated. Use 'new H2OXGBoost()'!")
      algo
    }
  }

  implicit class H2OAutoMLWithImplicits(algo: H2OAutoML) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2OAutoML = {
      logWarning("The constructor 'new H2OAutoML()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'" +
        " is deprecated. Use 'new H2OAutoML()'!")
      algo
    }
  }

  implicit class H2OGridSearchWithImplicits(algo: H2OGridSearch) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2OGridSearch = {
      logWarning("The constructor 'new H2OGridSearch()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'" +
        " is deprecated. Use 'new H2OGridSearch()'!")
      algo
    }
  }
}
