package py_sparkling.algos

import hex.tree.gbm.GBMModel.GBMParameters
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.algos.{H2OAlgorithmReader, H2OGBMParams}
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.SQLContext

/**
  * H2O GBM Wrapper for PySparkling
  */
class H2OGBM(parameters: Option[GBMParameters], override val uid: String)
            (implicit h2oContext: H2OContext, sqlContext: SQLContext)
  extends org.apache.spark.ml.h2o.algos.H2OGBM(parameters, uid)(h2oContext, sqlContext)
    with H2OGBMParams {

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("gbm"))

  def this(uid: String, hc: H2OContext, sqlContext: SQLContext) = this(None, uid)(hc, sqlContext)

  def this(parameters: GBMParameters)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), Identifiable.randomUID("gbm"))

  def this(parameters: GBMParameters, uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), uid)

}

object H2OGBM extends MLReadable[H2OGBM] {

  private final val defaultFileName = "gbm_params"

  override def read: MLReader[H2OGBM] = H2OAlgorithmReader.create[H2OGBM, GBMParameters](defaultFileName)

  override def load(path: String): H2OGBM = super.load(path)
}