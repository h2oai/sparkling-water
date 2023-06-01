package ai.h2o.sparkling.ml.models

class H2OUnsupervisedMOJOModel(override val uid: String) extends H2OAlgorithmMOJOModel(uid)

object H2OUnsupervisedMOJOModel extends H2OSpecificMOJOLoader[H2OUnsupervisedMOJOModel]
