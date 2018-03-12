package org.apache.spark.examples.h2o

import java.io.File

object TestUtils {
  def locate(name: String): String = {
    val abs = new File("/home/0xdiag/" + name)
    if (abs.exists()) {
      abs.getAbsolutePath
    } else {
      new File("./examples/" + name).getAbsolutePath
    }
  }

}
