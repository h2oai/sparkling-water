package ai.h2o.sparkling.repl

import java.io.{ByteArrayOutputStream, PrintStream}

class IntpConsoleStream() extends PrintStream(new ByteArrayOutputStream()) {
  def reset(): Unit = {
    out.asInstanceOf[ByteArrayOutputStream].reset()
  }

  def content: String = {
    out.toString
  }
}
