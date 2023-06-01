package ai.h2o.sparkling.repl

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.util.matching.Regex

/**
  * Test that patcher is patching.
  */
@RunWith(classOf[JUnitRunner])
class PatchUtilsTestSuite extends FunSuite with BeforeAndAfterAll {

  val EXAMPLE_CLASS_NAME = "intp_id_12$line1.$read$$iw$$iw"
  val EXAMPLE_RESULT_AFTER_PATCH = "intp_id_12$line1.$read"
  val FAILED_MATCH = "FAIL"

  def assertMatch(regex: Regex, input: String, exp: String): Unit = {
    val result = input match {
      case regex(b) => b
      case _ => FAILED_MATCH
    }
    assert(result == exp)
  }

  test("Test new regexp for OuterScopes") {
    val regex = PatchUtils.OUTER_SCOPE_REPL_REGEX
    assertMatch(regex, EXAMPLE_CLASS_NAME, EXAMPLE_RESULT_AFTER_PATCH)
    assertMatch(regex, "$line1.$read$$iw$$iw", "$line1.$read")
  }

  test("[SW-386] Test patched OuterScopes") {
    PatchUtils.PatchManager.patch("SW-386", Thread.currentThread().getContextClassLoader)

    val regexAfterPatch = getRegexp
    assertMatch(regexAfterPatch, EXAMPLE_CLASS_NAME, EXAMPLE_RESULT_AFTER_PATCH)
  }

  private def getRegexp: Regex = {
    val clz = Class.forName(PatchUtils.OUTER_SCOPES_CLASS + "$")
    val module = PatchUtils.getModule(clz)
    val f = clz.getDeclaredField("REPLClass")
    f.setAccessible(true)
    f.get(module).asInstanceOf[Regex]
  }
}
