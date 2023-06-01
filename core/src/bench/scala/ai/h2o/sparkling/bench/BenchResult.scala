package ai.h2o.sparkling.bench

import scala.concurrent.duration.TimeUnit

case class BenchResult(mean: Float, stdDev: Float, min: Float, max: Float, unit: TimeUnit) {
  def show(): String = {
    f"$mean%4f ± $stdDev%4f ($min%4f, $max%4f)"
  }
}

object BenchResult {
  def apply(measurements: Array[Long], inputUnit: TimeUnit, outputUnit: TimeUnit): BenchResult = {
    val convMeasurements = measurements.map(x => outputUnit.convert(x, inputUnit))
    val mean = convMeasurements.sum.toFloat / convMeasurements.length
    val stdev =
      Math.sqrt(convMeasurements.map(x => (x - mean) * (x - mean)).sum / (convMeasurements.length - 1)).toFloat
    new BenchResult(mean, stdev, convMeasurements.min, convMeasurements.max, outputUnit)
  }
}
