package ai.h2o.sparkling.ml

import ai.h2o.sparkling.ml.algos.H2OSupervisedAlgorithm
import hex.Model
import org.apache.spark.ml.param.{IntParam, LongParam, Param}

object ParameterSetters {
  implicit class AlgorithmWrapper(val algo: H2OSupervisedAlgorithm[_ <: Model.Parameters]) {

    def setSeed(value: Long): algo.type = setParam[Long, LongParam]("seed", value)

    def setNfolds(value: Int): algo.type = setParam[Int, IntParam]("nfolds", value)

    private def setParam[ValueType, ParamType <: Param[ValueType]](paramName: String, value: ValueType): algo.type = {
      val field = algo.getClass.getDeclaredFields.find(_.getName().endsWith("$$" + paramName)).head
      field.setAccessible(true)
      val parameter = field.get(algo).asInstanceOf[ParamType]
      algo.set(parameter, value)
      field.setAccessible(false)
      algo
    }
  }
}
