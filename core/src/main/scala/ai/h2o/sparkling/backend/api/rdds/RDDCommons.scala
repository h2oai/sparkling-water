package ai.h2o.sparkling.backend.api.rdds

import ai.h2o.sparkling.utils.SparkSessionUtils
import water.exceptions.H2ONotFoundArgumentException

trait RDDCommons {
  def validateRDDId(rddId: Int): Unit = {
    SparkSessionUtils.active.sparkContext.getPersistentRDDs
      .getOrElse(rddId, throw new H2ONotFoundArgumentException(s"RDD with ID '$rddId' does not exist!"))
  }
}
