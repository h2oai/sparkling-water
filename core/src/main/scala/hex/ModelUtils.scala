package hex

import hex.Model.Output
import water.util.ArrayUtils

/**
 * Simple access method
 */
object ModelUtils {

  def classify(row: Array[Double], m : Model[_, _, _]): (String, Array[Double]) = {
    val modelOutput = m._output.asInstanceOf[Output]
    val nclasses = modelOutput.nclasses()
    val classNames = modelOutput.classNames()
    val pred = m.score0(row, new Array[Double](nclasses + 1))
    val predProb = pred slice (1, pred.length)
    val maxProbIdx = ArrayUtils.maxIndex(predProb)
    (classNames(maxProbIdx), predProb)
  }
}
