package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.IntParam

/**
  * Parameters available on the tree-based MOJO Model
  */
trait H2OTreeBasedMOJOParams extends H2OBaseMOJOParams {
  protected final val ntrees = new IntParam(this, "ntrees", "Number of trees representing the model")

  setDefault(ntrees -> -1)

  def getNtrees(): Int = $(ntrees)
}
