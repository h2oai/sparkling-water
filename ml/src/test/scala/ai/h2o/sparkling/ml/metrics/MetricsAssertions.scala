package ai.h2o.sparkling.ml.metrics

import org.scalatest.Matchers

object MetricsAssertions extends Matchers {
  def assertMetricsObjectAgainstMetricsMap(metricsObject: H2OMetrics, metrics: Map[String, Double]): Unit = {
    for (getter <- metricsObject.getClass.getMethods
         if getter.getName.startsWith("get")
         if getter.getName != "getCustomMetricValue"
         if getter.getParameterCount == 0
         if getter.getReturnType.isPrimitive) {
      val value = getter.invoke(metricsObject)
      val metricName = getter.getName.substring(3)
      assert(metrics.contains(metricName), s"$metricName is not defined in H2OMetrics.")
      val metricValue = metrics.get(metricName).get
      if (metricValue.isNaN) {
        assert(value.asInstanceOf[Double].isNaN)
      } else {
        value shouldEqual metricValue
      }
    }
  }
}
