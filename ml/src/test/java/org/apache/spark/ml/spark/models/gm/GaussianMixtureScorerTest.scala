package org.apache.spark.ml.spark.models.gm

import java.lang.{Double => JDouble}

import org.scalatest.FunSuite

class GaussianMixtureScorerTest extends FunSuite {

  test("Multiply matrix and a vector") {
    val matrix: Array[Array[JDouble]] = new Array[Array[JDouble]](3)
    matrix(0) = Array[JDouble](1.0,2.0,3.0)
    matrix(1) = Array[JDouble](4.0,5.0,6.0)
    matrix(2) = Array[JDouble](7.0,8.0,9.0)
    val vec = Array[Double](1.0,2.0,3.0)
    val res = GaussianMixtureScorer.multiplyMV(matrix, vec)
    val expected = Array[Double](14.0,32.0,50.0)
    assertResult(expected)(res)
  }

  test("Multiply two vectors") {
    val vec1 = Array[Double](1,2,3)
    val vec2 = Array[Double](4,5,6)
    val res = GaussianMixtureScorer.multiplyV(vec1, vec2)
    assertResult(32)(res)
  }

  test("Compute delta") {
    assertResult(
      Array(1,1,3.3)
    )(
      GaussianMixtureScorer.delta(Array(1.1, 1.25, 3.44), Array(.1, .25, .14))
    )
  }

}
