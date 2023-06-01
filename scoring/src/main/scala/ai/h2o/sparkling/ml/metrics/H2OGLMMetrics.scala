package ai.h2o.sparkling.ml.metrics

trait H2OGLMMetrics extends H2OMetrics {

  /**
    * residual deviance.
    */
  def getResidualDeviance(): Double

  /**
    * null deviance.
    */
  def getNullDeviance(): Double

  /**
    * AIC.
    */
  def getAIC(): Double

  /**
    * null DOF.
    */
  def getNullDegreesOfFreedom(): Long

  /**
    * residual DOF.
    */
  def getResidualDegreesOfFreedom(): Long
}
