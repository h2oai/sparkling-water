package ai.h2o.sparkling.backend.api.dataframes

import ai.h2o.sparkling.utils.SparkSessionUtils
import water.exceptions.H2ONotFoundArgumentException

trait DataFrameCommons {
  def validateDataFrameId(dataFrameId: String): Unit = {
    if (!SparkSessionUtils.active.sqlContext.tableNames().contains(dataFrameId)) {
      throw new H2ONotFoundArgumentException(s"DataFrame with id '$dataFrameId' does not exist!")
    }
  }
}
