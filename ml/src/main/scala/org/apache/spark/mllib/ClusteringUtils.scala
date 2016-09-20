/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.mllib

import breeze.linalg.{diag, eigSym, max, DenseMatrix => DBM, DenseVector => DBV}
import org.apache.spark.ml.spark.models.gm.GaussianMixtureModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.util.MLUtils
import water.util.TwoDimTable

object ClusteringUtils {

  lazy val EPSILON = MLUtils.EPSILON

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

  // Copy paste of a private Spark MultivariateGaussian#calculateCovarianceConstants method
  def calculateCovarianceConstants(sigma: Matrix, mu: Vector): (Array[Array[java.lang.Double]], java.lang.Double) = {
    val eigSym.EigSym(d, u) = eigSym(sigma.toBreeze.toDenseMatrix)

    val tol = MLUtils.EPSILON * max(d) * d.length

    try {
      val logPseudoDetSigma = d.activeValuesIterator.filter(_ > tol).map(math.log).sum

      val pinvS = diag(new DBV(d.map(v => if (v > tol) math.sqrt(1.0 / v) else 0.0).toArray))

      val covScala = pinvS * u.t
      val cov = new Array[Array[java.lang.Double]](covScala.rows)

      for (i <- 0 until covScala.rows) {
        cov(i) = new Array[java.lang.Double](covScala.cols)
        for (j <- 0 until covScala.cols) {
          cov(i)(j) = covScala(i,j)
        }
      }

      (cov, -0.5 * (mu.size * math.log(2.0 * math.Pi) + logPseudoDetSigma))
    } catch {
      case uex: UnsupportedOperationException =>
        throw new IllegalArgumentException("Covariance matrix has no non-zero singular values")
    }
  }

}
