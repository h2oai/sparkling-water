package water.parser

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class CategoricalPreviewParseWriterTestSuite extends FunSuite with Matchers {

  val testCases = Seq(
    Seq.empty[String],
    Seq(null, null),
    Seq(null, "word", null),
    Seq("a", "b", "c", "d", "e", "f", "g", null),
    Seq("a", "b", "b", "a", "a"),
    Seq(null, "a", null, "b", null, "b", null, "a", null, "a", null))

  for (testCase <- testCases) {
    test(s"CategoricalPreviewParseWriter returns expected result on $testCase") {
      val referenceWriter = new PreviewParseWriter(1)
      for (value <- testCase) {
        if (value == null) {
          referenceWriter.addInvalidCol(0)
        } else {
          referenceWriter.addStrCol(0, new BufferedString(value))
        }
        referenceWriter.newLine()
      }
      val expected = referenceWriter.guessTypes()(0)

      val domain = testCase.filter(_ != null).distinct.toArray
      val naCount = testCase.filter(_ == null).length
      val result = CategoricalPreviewParseWriter.guessType(domain, testCase.length, naCount)

      result shouldEqual expected
    }
  }
}
