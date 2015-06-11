package hex

import hex.Model.Output

/**
 * Simple access method
 */
object ModelUtils {

  def classify(row: Array[Double], m : Model[_, _, _]): (String, Array[Double]) = {
    val modelOutput = m._output.asInstanceOf[Output]
    val nclasses = modelOutput.nclasses()
    val classNames = modelOutput.classNames()
    val pred = m.score0(row, new Array[Double](nclasses + 1))
    (classNames(pred(0).asInstanceOf[Int]), pred slice (1, pred.length))
  }
}
