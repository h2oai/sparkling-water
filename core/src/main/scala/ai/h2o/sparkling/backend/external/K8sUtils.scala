package ai.h2o.sparkling.backend.external

trait K8sUtils {

  protected def convertLabel(label: String): String = {
    label.split("=").mkString(": ")
  }
}
