package ai.h2o.sparkling.backend.utils

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProgressBarTestSuite extends FunSuite with Matchers {

  test("should return a rendered progress bar - 0%") {
    val input = 0f

    val result = ProgressBar.renderProgressBar(progress = input)

    result shouldBe "|__________________________________________________| 0%"
  }

  test("should return a rendered progress bar - 55%") {
    val input = 0.555555f

    val result = ProgressBar.renderProgressBar(progress = input)

    result shouldBe "|████████████████████████████______________________| 55%"
  }

  test("should return a rendered progress bar - 100%") {
    val input = 1f

    val result = ProgressBar.renderProgressBar(progress = input)

    result shouldBe "|██████████████████████████████████████████████████| 100%"
  }

}
