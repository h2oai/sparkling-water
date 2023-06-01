package ai.h2o.sparkling.extensions.serde

import ai.h2o.sparkling.extensions.serde.ExpectedTypes.ExpectedType
import water.fvec.Vec

object SerdeUtils {
  private[sparkling] def expectedTypesToVecTypes(
      expectedTypes: Array[ExpectedType],
      vecElemSizes: Array[Int]): Array[Byte] = {
    var vecCount = 0
    expectedTypes.flatMap {
      case ExpectedTypes.Bool | ExpectedTypes.Byte | ExpectedTypes.Char | ExpectedTypes.Short | ExpectedTypes.Int |
          ExpectedTypes.Long | ExpectedTypes.Float | ExpectedTypes.Double =>
        Array(Vec.T_NUM)
      case ExpectedTypes.String => Array(Vec.T_STR)
      case ExpectedTypes.Categorical => Array(Vec.T_CAT)
      case ExpectedTypes.Timestamp => Array(Vec.T_TIME)
      case ExpectedTypes.Vector =>
        val result = Array.fill(vecElemSizes(vecCount))(Vec.T_NUM)
        vecCount += 1
        result
    }
  }
}
