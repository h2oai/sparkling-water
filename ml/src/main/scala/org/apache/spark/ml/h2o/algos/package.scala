package org.apache.spark.ml.h2o

import org.apache.spark.h2o.H2OContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext

package object algos extends Logging{

  implicit class H2OGBMWithImplicits(algo: H2OGBM) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2OGBM = {
      logWarning("The constructor 'new H2OGBM()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'. Use 'new H2OGBM()'!")
      algo
    }
  }

  implicit class H2OGLMWithImplicits(algo: H2OGLM) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2OGLM = {
      logWarning("The constructor 'new H2OGLM()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'. Use 'new H2OGLM()'!")
      algo
    }
  }

  implicit class H2ODeepLearningWithImplicits(algo: H2ODeepLearning) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2ODeepLearning = {
      logWarning("The constructor 'new H2ODeepLearning()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'. Use 'new H2ODeepLearning()'!")
      algo
    }
  }

  implicit class H2OXGboostWithImplicits(algo: H2OXGBoost) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2OXGBoost = {
      logWarning("The constructor 'new H2OXGBoost()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'. Use 'new H2OXGBoost()'!")
      algo
    }
  }

  implicit class H2OAutoMLWithImplicits(algo: H2OAutoML) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2OAutoML = {
      logWarning("The constructor 'new H2OAutoML()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'. Use 'new H2OAutoML()'!")
      algo
    }
  }

  implicit class H2OGridSearchWithImplicits(algo: H2OGridSearch) {
    def apply(h2oContext: H2OContext, sqlContext: SQLContext): H2OGridSearch = {
      logWarning("The constructor 'new H2OGridSearch()(implicit h2oContext: H2OContext, sqlContext: SQLContext)'. Use 'new H2OGridSearch()'!")
      algo
    }
  }
}
