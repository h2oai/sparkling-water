package org.apache.spark.h2o.backends

import org.scalatest.{FunSuite, Matchers}

class SharedBackendUtilsTestSuite extends FunSuite with Matchers {

  val expectedHttpHeaderArgs = Seq(
    "-add_http_header", "X-Frame-Options", "deny",
    "-add_http_header", "X-XSS-Protection", "X-XSS-Protection: 1; mode=block",
    "-add_http_header", "X-Content-Type-Options", "nosniff",
    "-add_http_header", "Content-Security-Policy", "default-src 'self' 'unsafe-eval' 'unsafe-inline'; img-src 'self' data:")

  test("parseStringToHttpArgs parses list of http headers with regular formatting") {
    val input =
      """X-Frame-Options: deny
        |X-XSS-Protection: X-XSS-Protection: 1; mode=block
        |X-Content-Type-Options: nosniff
        |Content-Security-Policy: default-src 'self' 'unsafe-eval' 'unsafe-inline'; img-src 'self' data:
      """.stripMargin

    val result = SharedBackendUtils.parseStringToHttpHeaderArgs(input)

    result shouldEqual expectedHttpHeaderArgs
  }

  test("parseStringToHttpArgs parses list of http headers with random formatting") {
    val input =
      """X-Frame-Options:     deny
        |        X-XSS-Protection: X-XSS-Protection: 1; mode=block
        |    X-Content-Type-Options:     nosniff
        |   Content-Security-Policy:default-src 'self' 'unsafe-eval' 'unsafe-inline'; img-src 'self' data:
      """.stripMargin

    val result = SharedBackendUtils.parseStringToHttpHeaderArgs(input)

    result shouldEqual expectedHttpHeaderArgs
  }

  test("parseStringToHttpArgs parses list of http headers with empty new lines") {
    val input =
      """
        |
        |
        |X-Frame-Options: deny
        |
        |X-XSS-Protection: X-XSS-Protection: 1; mode=block
        |
        |
        |
        |X-Content-Type-Options: nosniff
        |
        |
        |Content-Security-Policy: default-src 'self' 'unsafe-eval' 'unsafe-inline'; img-src 'self' data:
        |
        |
      """.stripMargin

    val result = SharedBackendUtils.parseStringToHttpHeaderArgs(input)

    result shouldEqual expectedHttpHeaderArgs
  }

  test("parseStringToHttpArgs parses empty lines") {
    val input =
      """
        |
        |
        |
      """.stripMargin

    val result = SharedBackendUtils.parseStringToHttpHeaderArgs(input)

    result shouldEqual Seq.empty[String]
  }

  test("parseStringToHttpArgs parses empty string") {
    val input = ""

    val result = SharedBackendUtils.parseStringToHttpHeaderArgs(input)

    result shouldEqual Seq.empty[String]
  }
}
