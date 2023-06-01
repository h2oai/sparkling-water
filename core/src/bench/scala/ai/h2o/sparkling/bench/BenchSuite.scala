package ai.h2o.sparkling.bench

import java.util.concurrent.TimeUnit

import org.scalatest.FunSuite

import scala.concurrent.duration.TimeUnit

class BenchSuite extends FunSuite {

  protected def benchTest(
      testName: String,
      iterations: Int = 5,
      warmUp: Int = 1,
      outputTimeUnit: TimeUnit = TimeUnit.MILLISECONDS)(testFun: => Unit): Unit = {
    def body(): Unit = {
      val result = bench(iterations, warmUp, outputTimeUnit) {
        testFun
      }
      println(s"$testName: ${result.show()}")
    }
    registerTest(testName)(body())
  }

  /**
    * Measure execution time of given block in nanoseconds.
    *
    * @param block block to measure
    * @return number of ns to execute given block
    */
  private def timer(block: => Unit): Long = {
    val now = System.nanoTime()
    block
    System.nanoTime() - now
  }

  /**
    * Benchmark given block of code.
    *
    * @param iterations number of iterations to execute the block of code
    * @param block      block to execute as benchmark
    * @return
    */
  private def bench(iterations: Int, warmup: Int = 4, outputTimeUnit: TimeUnit = TimeUnit.MILLISECONDS)(
      block: => Unit): BenchResult = {
    val times = new Array[Long](iterations)
    // Warmup
    for (_ <- 0 until warmup) {
      timer(block)
    }
    // Measure
    for (i <- 0 until iterations) {
      times(i) = timer(block)
    }

    BenchResult(times, TimeUnit.NANOSECONDS, outputTimeUnit)
  }
}
