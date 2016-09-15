package org.apache.spark.mllib

import org.apache.spark.ml.spark.models.gm.GaussianMixtureModel
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vectors}
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.util.MLUtils
import water.util.TwoDimTable

object ClusteringUtils {

  def create2DTable(data: Array[Array[Double]], colHeaderForRowHeader: String, colHeaders: Array[String], name: String): TwoDimTable = {
    if (data == null || data.length == 0) {
      return new TwoDimTable(name, null, Array[String](), Array[String](), Array[String](), Array[String](), colHeaderForRowHeader)
    }

    val rowHeaders: Array[String] = data.indices.map(i => String.valueOf(i + 1)).toArray

    val colTypes: Array[String] = Array.fill(data(0).length)("double")
    val colFormats: Array[String] = Array.fill(data(0).length)("%f")

    val table: TwoDimTable = new TwoDimTable(name, null, rowHeaders, colHeaders, colTypes, colFormats, colHeaderForRowHeader)
    var i: Int = 0

    for {
      i <- data.indices
      j <- data(i).indices
    } {
      table.set(i, j, data(i)(j))
    }

    table
  }

  def perRowGaussianMixture(data: Array[Float], gmm: GaussianMixtureModel): Double = perRowGaussianMixture(data.map(_.toDouble), gmm)

  def perRowGaussianMixture(data: Array[Double], gmm: GaussianMixtureModel): Double = {
    val dists = gmm._output._sigma.zip(gmm._output._sigma_cols).map { case (mat, col) =>
      Matrices.dense(mat.length / col, col, mat).asInstanceOf[DenseMatrix]
    }.zip(gmm._output._mu).map { case (mat, mu) =>
      new MultivariateGaussian(Vectors.dense(mu), mat)
    }

    gmm._output._weights.zip(dists).map {
      case (weight, dist) => MLUtils.EPSILON + weight * dist.pdf(Vectors.dense(data.take(dist.mu.size)))
    }.sum
  }

}
