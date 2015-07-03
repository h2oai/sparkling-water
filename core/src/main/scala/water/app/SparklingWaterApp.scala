package water.app

import hex.deeplearning.DeepLearningParameters.Activation
import hex.deeplearning.{DeepLearningParameters, DeepLearning, DeepLearningModel}
import hex.tree.gbm.GBMModel
import hex.tree.gbm.GBMModel.GBMParameters.Family
import hex.{Model, ModelMetrics}
import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import water.Key
import water.fvec.Frame

/**
 * A simple application trait to define Sparkling Water applications.
 */
trait SparklingWaterApp {
  @transient val sc: SparkContext
  @transient val sqlContext: SQLContext
  @transient val h2oContext: H2OContext

  def loadH2OFrame(datafile: String) = new H2OFrame(new java.net.URI(datafile))

  def shutdown(): Unit = {
    // Shutdown Spark
    sc.stop()
    // Shutdown H2O explicitly (at least the driver)
    h2oContext.stop()
  }
}

// FIXME: should be published by h2o-scala interface
trait ModelMetricsSupport {
  def r2(model: GBMModel, fr: Frame) =  hex.ModelMetrics.getFromDKV(model, fr).asInstanceOf[hex.ModelMetricsSupervised].r2()

  def modelMetrics[T <: ModelMetrics, M <: Model[M,P,O], P <: hex.Model.Parameters, O <: hex.Model.Output]
  (model: Model[M,P,O], fr: Frame) = ModelMetrics.getFromDKV(model, fr).asInstanceOf[T]

  def binomialMM[M <: Model[M,P,O], P <: hex.Model.Parameters, O <: hex.Model.Output]
  (model: Model[M,P,O], fr: Frame) = modelMetrics[hex.ModelMetricsBinomial,M,P,O](model, fr)

  def multinomialMM[M <: Model[M,P,O], P <: hex.Model.Parameters, O <: hex.Model.Output]
  (model: Model[M,P,O], fr: Frame) = modelMetrics[hex.ModelMetricsMultinomial,M,P,O](model, fr)
}

// Create companion object
object ModelMetricsSupport extends ModelMetricsSupport

trait DeepLearningSupport {

  def DLModel(train: H2OFrame, valid: H2OFrame, response: String,
              epochs: Int = 10, l1: Double = 0.0001, l2: Double = 0.0001,
              activation: Activation = Activation.RectifierWithDropout,
              hidden:Array[Int] = Array(200,200)): DeepLearningModel = {

    val dlParams = new DeepLearningParameters()
    dlParams._train = train._key
    dlParams._valid = if (valid != null) valid._key else null
    dlParams._response_column = response
    dlParams._epochs = epochs
    dlParams._l1 = l1
    dlParams._l2 = l2
    dlParams._activation = activation
    dlParams._hidden = hidden

    // Create a job
    val dl = new DeepLearning(dlParams)
    val model = dl.trainModel.get
    model
  }
}

// Create companion object
object DeepLearningSupport extends DeepLearningSupport

trait GBMSupport {

  def GBMModel(train: H2OFrame, test: H2OFrame, response: String,
               modelId: String = "model",
               ntrees:Int = 50, depth:Int = 6, family: Family = Family.AUTO): GBMModel = {
    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters

    val gbmParams = new GBMParameters()
    gbmParams._model_id = Key.make(modelId).asInstanceOf[Key[Frame]]
    gbmParams._train = train._key
    gbmParams._valid = if (test != null) test._key else null
    gbmParams._response_column = response
    gbmParams._ntrees = ntrees
    gbmParams._max_depth = depth
    gbmParams._distribution = family

    val gbm = new GBM(gbmParams)
    val model = gbm.trainModel.get
    model
  }
}

// Create companion object
object GBMSupport extends GBMSupport

