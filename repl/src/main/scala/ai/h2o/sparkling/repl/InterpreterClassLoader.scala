package ai.h2o.sparkling.repl

import java.util.Scanner

/**
  * Interpreter classloader which allows multiple interpreters to coexist
  */
class InterpreterClassLoader(val fallbackClassloader: ClassLoader) extends ClassLoader {

  override def loadClass(name: String): Class[_] = {
    if (name.startsWith("intp_id")) {
      val intp_id = new Scanner(name).useDelimiter("\\D+").nextInt()
      H2OIMain.existingInterpreters(intp_id).classLoader.loadClass(name)
    } else {
      fallbackClassloader.loadClass(name)
    }
  }
}
