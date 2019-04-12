package water.api

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.util.DeprecatedMethod

import scala.language.experimental.macros

@RunWith(classOf[JUnitRunner])
class DeprecateMacroSuite extends FunSuite {

  class VariousAnnotationUsages {
    @DeprecatedMethod(replacement = "replacing method")
    def method1: Unit = println("method1")

    @DeprecatedMethod("replacing method")
    def method2: Unit = println("mehtod2")

    @DeprecatedMethod()
    def method3: Unit = println("method3")

    @DeprecatedMethod
    def method4: Unit = println("mehtod4")
  }

  test("Various annotation usages") {
    val vau = new VariousAnnotationUsages()
    vau.method1
    vau.method2
    vau.method3
    vau.method4
  }

  class VariousAnnotatedMethods {
    @DeprecatedMethod(replacement = "replacing method")
    def method1: String = "method1"

    @DeprecatedMethod(replacement = "replacing method")
    def method2(parameter:String): Unit = println(parameter)

    @DeprecatedMethod(replacement = "replacing method")
    def method3[T](parameter: T): T = parameter

    @DeprecatedMethod(replacement = "replacing method")
    private[DeprecateMacroSuite] def method4: Unit = println("mehtod4")
  }

  test("Various annotated methods") {
    val vam = new VariousAnnotatedMethods()
    vam.method1
    vam.method2("method2")
    vam.method3("method3")
    vam.method4
  }
}