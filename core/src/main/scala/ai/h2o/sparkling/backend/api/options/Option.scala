package ai.h2o.sparkling.backend.api.options

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest
import water.exceptions.H2ONotFoundArgumentException

/** Schema representing /3/option endpoint */
case class Option(name: String, value: String)

object Option extends ParameterBase {
  private[options] case class OptionParameters(name: String) {
    def validate(conf: H2OConf): Unit = {
      if (!conf.contains(name)) {
        throw new H2ONotFoundArgumentException(s"The option '$name' is not specified on the H2OConf!")
      }
    }
  }

  private[options] object OptionParameters {
    def parse(request: HttpServletRequest): OptionParameters = {
      val name = getParameterAsString(request, "name")
      OptionParameters(name)
    }
  }
}
