package ai.h2o.sparkling.repl

import java.io.StringWriter

import scala.tools.nsc.interpreter.JPrintWriter

class IntpResponseWriter() extends JPrintWriter(new StringWriter()) {
  def reset(): Unit = {
    out.asInstanceOf[StringWriter].getBuffer.setLength(0)
  }

  def content: String = {
    out.toString
  }

  override def write(s: String): Unit = {
    // when running tests, store whole interpreter response
    if (!sys.props.contains("spark.testing")) {
      reset()
    }
    super.write(s)
  }
}
