package ai.h2o.sparkling.api.generation.python

import java.lang.reflect.Method
import scala.collection.immutable.Map

object ConfigurationTemplate extends ((Array[Method], Array[Method], Map[String, List[Int]], Class[_]) => String) {

  def apply(
      getters: Array[Method],
      setters: Array[Method],
      settersArityMap: Map[String, List[Int]],
      entity: Class[_]): String = {

    s"""import warnings
       |from ai.h2o.sparkling.SharedBackendConfUtils import SharedBackendConfUtils
       |
       |
       |class ${entity.getSimpleName}(SharedBackendConfUtils):
       |
       |    #
       |    # Getters
       |    #
       |
       |${generateGetters(getters)}
       |
       |    #
       |    # Setters
       |    #
       |
       |${generateSetters(setters, settersArityMap)}""".stripMargin
  }

  private def generateGetters(getters: Array[Method]): String = {
    getters.map(generateGetter).mkString("\n")
  }

  private def generateSetters(setters: Array[Method], settersArityMap: Map[String, List[Int]]): String = {
    setters.map(generateSetter(_, settersArityMap)).mkString("\n")
  }

  private def generateGetter(m: Method): String = {
    s"""    def ${m.getName}(self):
       |        ${getReturnLine(m)}
       |""".stripMargin
  }

  private def generateSetter(m: Method, settersArityMap: Map[String, List[Int]]): String = {
    val arities = settersArityMap(m.getName)
    val overloaded = arities.length > 1
    if (overloaded) {
      s"""    def ${m.getName}(self, *args):
         |        self._jconf.${m.getName}(*args)
         |        return self
         |""".stripMargin
    } else {
      val arity = arities.head
      if (arity == 0) {
        s"""    def ${m.getName}(self):
           |        self._jconf.${m.getName}()
           |        return self
           |""".stripMargin
      } else {
        val parameterNames = m.getParameters.map(_.getName)
        val parameters = parameterNames.mkString(",")
        s"""    def ${m.getName}(self, ${parameters}):
           |        self._jconf.${m.getName}(${parameters})
           |        return self
           |""".stripMargin
      }
    }
  }

  private def getReturnLine(m: Method): String = {
    m.getReturnType match {
      case clz if clz.getName == "scala.Option" => s"return self._get_option(self._jconf.${m.getName}())"
      case _ => s"return self._jconf.${m.getName}()"
    }
  }
}
