package org.apache.spark.h2o.backends

import org.scalatest.{FunSuite, Matchers}

class SharedBackendUtilsTestSuite extends FunSuite with Matchers {

  private val expectedHttpHeaderArgs = Seq(
    "-add_http_header", "X-Request-ID", "f058ebd6-02f7-4d3f-942e-904344e8cde5",
    "-add_http_header", "X-Csrf-Token", "i8XNjC4b8KVok4uw5RftR38Wgp2BFwq",
    "-add_http_header", "X-Forwarded-For", "129.78.138.66, 129.78.64.103",
    "-add_http_header", "Content-Security-Policy-Report-Only", "default-src 'none'; style-src cdn.example.com:8080; report-uri /_/csp-reports")

  test("parseStringToHttpArgs parses list of http headers with regular formatting") {
    val input =
      """X-Request-ID: f058ebd6-02f7-4d3f-942e-904344e8cde5
        |X-Csrf-Token: i8XNjC4b8KVok4uw5RftR38Wgp2BFwq
        |X-Forwarded-For: 129.78.138.66, 129.78.64.103
        |Content-Security-Policy-Report-Only: default-src 'none'; style-src cdn.example.com:8080; report-uri /_/csp-reports
      """.stripMargin

    val result = SharedBackendUtils.parseStringToHttpHeaderArgs(input)

    result shouldEqual expectedHttpHeaderArgs
  }

  test("parseStringToHttpArgs parses list of http headers with random formatting") {
    val input =
      """   X-Request-ID:    f058ebd6-02f7-4d3f-942e-904344e8cde5
        |   X-Csrf-Token:   i8XNjC4b8KVok4uw5RftR38Wgp2BFwq
        |X-Forwarded-For:           129.78.138.66, 129.78.64.103
        |Content-Security-Policy-Report-Only:default-src 'none'; style-src cdn.example.com:8080; report-uri /_/csp-reports
      """.stripMargin

    val result = SharedBackendUtils.parseStringToHttpHeaderArgs(input)

    result shouldEqual expectedHttpHeaderArgs
  }

  test("parseStringToHttpArgs parses list of http headers with empty new lines") {
    val input =
      """
        |
        |
        |X-Request-ID: f058ebd6-02f7-4d3f-942e-904344e8cde5
        |
        |
        |X-Csrf-Token: i8XNjC4b8KVok4uw5RftR38Wgp2BFwq
        |
        |X-Forwarded-For: 129.78.138.66, 129.78.64.103
        |Content-Security-Policy-Report-Only: default-src 'none'; style-src cdn.example.com:8080; report-uri /_/csp-reports
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
