/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.ml.h2o.algos


import hex.StringPair
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.MissingValuesHandling
import hex.glm.GLM
import hex.glm.GLMModel.GLMParameters
import hex.glm.GLMModel.GLMParameters.{Family, Link, Solver}
import hex.schemas.GLMV3.GLMParametersV3
import org.apache.spark.annotation.Since
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.models._
import org.apache.spark.ml.h2o.param.{EnumParam, H2OAlgoParams}
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.SQLContext
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JValue}
import water.AutoBuffer
import water.support.ModelSerializationSupport

/**
  * H2O GLM algorithm exposed via Spark ML pipelines.
  */
class H2OGLM(parameters: Option[GLMParameters], override val uid: String)
            (implicit h2oContext: H2OContext, sqlContext: SQLContext)
  extends H2OAlgorithm[GLMParameters, H2OMOJOModel](parameters)
    with H2OGLMParams {

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("glm"))

  def this(uid: String, hc: H2OContext, sqlContext: SQLContext) = this(None, uid)(hc, sqlContext)

  def this(parameters: GLMParameters)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), Identifiable.randomUID("glm"))

  def this(parameters: GLMParameters, uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), uid)

  override def defaultFileName: String = H2OGLM.defaultFileName

  override def trainModel(params: GLMParameters): H2OMOJOModel = {
    val model = new GLM(params).trainModel().get()
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(model))
  }

}

object H2OGLM extends MLReadable[H2OGLM] {

  private final val defaultFileName = "glm_params"

  @Since("1.6.0")
  override def read: MLReader[H2OGLM] = H2OAlgorithmReader.create[H2OGLM, GLMParameters](defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2OGLM = super.load(path)
}


/**
  * Parameters for Spark's API exposing underlying H2O model.
  */
trait H2OGLMParams extends H2OAlgoParams[GLMParameters] {

  type H2O_SCHEMA = GLMParametersV3

  protected def paramTag = reflect.classTag[GLMParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //
  final val standardize = booleanParam("standardize")
  final val family = new H2OGLMFamilyParam(this, "family", "family")
  final val link = new H2OGLMLinkParam(this, "link", "link")
  final val solver = new H2OGLMSolverParam(this, "solver", "solver")
  final val tweedieVariancePower = doubleParam("tweedieVariancePower")
  final val tweedieLinkPower = doubleParam("tweedieLinkPower")
  final val alpha = nullableDoubleArrayParam("alpha")
  final val lambda_ = nullableDoubleArrayParam("lambda_", "lambda")
  final val missingValuesHandling = new H2OGLMMissingValuesHandlingParam(this, "missingValuesHandling", "missingValuesHandling")
  final val prior = doubleParam("prior")
  final val lambdaSearch = booleanParam("lambdaSearch")
  final val nlambdas = intParam("nlambdas")
  final val nonNegative = booleanParam("nonNegative")
  final val exactLambdas = booleanParam("exactLambdas", "exact lambdas")
  final val lambdaMinRatio = doubleParam("lambdaMinRatio")
  final val maxIterations = intParam("maxIterations")
  final val intercept = booleanParam("intercept")
  final val betaEpsilon = doubleParam("betaEpsilon")
  final val objectiveEpsilon = doubleParam("objectiveEpsilon")
  final val gradientEpsilon = doubleParam("gradientEpsilon")
  final val objReg = doubleParam("objReg")
  final val computePValues = booleanParam("computePValues")
  final val removeCollinearColumns = booleanParam("removeCollinearColumns")
  final val interactions = nullableStringArrayParam("interactions")
  final val interactionPairs = new H2OGLMStringPairArrayParam(this, "interactionPairs", "interactionPairs")
  final val earlyStopping = booleanParam("earlyStopping")

  //
  // Default values
  //
  setDefault(
    standardize -> true,
    family -> Family.gaussian,
    link -> Link.family_default,
    solver -> Solver.AUTO,
    tweedieVariancePower -> 0,
    tweedieLinkPower -> 0,
    alpha -> null,
    lambda_ -> null,
    missingValuesHandling -> MissingValuesHandling.MeanImputation,
    prior -> -1,
    lambdaSearch -> false,
    nlambdas -> -1,
    nonNegative -> false,
    exactLambdas -> false,
    lambdaMinRatio -> -1,
    maxIterations -> -1,
    intercept -> true,
    betaEpsilon -> 1e-4,
    objectiveEpsilon -> -1,
    gradientEpsilon -> -1,
    objReg -> -1,
    computePValues -> false,
    removeCollinearColumns -> false,
    interactions -> null,
    interactionPairs -> null,
    earlyStopping -> true
  )

  //
  // Getters
  //
  /** @group getParam */
  def getStandardize() = $(standardize)

  /** @group getParam */
  def getFamily() = $(family)

  /** @group getParam */
  def getLink() = $(link)

  /** @group getParam */
  def getSolver() = $(solver)

  /** @group getParam */
  def getTweedieVariancePower() = $(tweedieVariancePower)

  /** @group getParam */
  def getTweedieLinkPower() = $(tweedieLinkPower)

  /** @group getParam */
  def getAlpha() = $(alpha)

  /** @group getParam */
  def getLambda() = $(lambda_)

  /** @group getParam */
  def getMissingValuesHandling() = $(missingValuesHandling)

  /** @group getParam */
  def getPrior() = $(prior)

  /** @group getParam */
  def getLambdaSearch() = $(lambdaSearch)

  /** @group getParam */
  def getNlambdas() = $(nlambdas)

  /** @group getParam */
  def getNonNegative() = $(nonNegative)

  /** @group getParam */
  def getExactLambdas() = $(exactLambdas)

  /** @group getParam */
  def getLambdaMinRatio() = $(lambdaMinRatio)

  /** @group getParam */
  def getMaxIterations() = $(maxIterations)

  /** @group getParam */
  def getIntercept() = $(intercept)

  /** @group getParam */
  def getBetaEpsilon() = $(betaEpsilon)

  /** @group getParam */
  def getObjectiveEpsilon() = $(objectiveEpsilon)

  /** @group getParam */
  def getGradientEpsilon() = $(gradientEpsilon)

  /** @group getParam */
  def getObjReg() = $(objReg)

  /** @group getParam */
  def getComputePValues() = $(computePValues)

  /** @group getParam */
  def getRemoteCollinearColumns() = $(removeCollinearColumns)

  /** @group getParam */
  def getInteractions() = $(interactions)

  /** @group getParam */
  def getInteractionPairs() = $(interactionPairs)

  /** @group getParam */
  def getEarlyStopping() = $(earlyStopping)


  //
  // Setters
  //
  /** @group setParam */
  def setStandardize(value: Boolean): this.type = set(standardize, value)

  /** @group setParam */
  def setFamily(value: Family): this.type = set(family, value)

  /** @group setParam */
  def setLink(value: Link): this.type = set(link, value)

  /** @group setParam */
  def setSolver(value: Solver): this.type = set(solver, value)

  /** @group setParam */
  def setTweedieVariancePower(value: Double): this.type = set(tweedieVariancePower, value)

  /** @group setParam */
  def setTweedieLinkPower(value: Double): this.type = set(tweedieLinkPower, value)

  /** @group setParam */
  def setAlpha(value: Array[Double]): this.type = set(alpha, value)

  /** @group setParam */
  def setLambda(value: Array[Double]): this.type = set(lambda_, value)

  /** @group setParam */
  def setMissingValuesHandling(value: MissingValuesHandling): this.type = set(missingValuesHandling, value)

  /** @group setParam */
  def setPrior(value: Double): this.type = set(prior, value)

  /** @group setParam */
  def setLambdaSearch(value: Boolean): this.type = set(lambdaSearch, value)

  /** @group setParam */
  def setNlambdas(value: Int): this.type = set(nlambdas, value)

  /** @group setParam */
  def setNonNegative(value: Boolean): this.type = set(nonNegative, value)

  /** @group setParam */
  def setExactLambdas(value: Boolean): this.type = set(exactLambdas, value)

  /** @group setParam */
  def setLambdaMinRatio(value: Double): this.type = set(lambdaMinRatio, value)

  /** @group setParam */
  def setMaxIterations(value: Int): this.type = set(maxIterations, value)

  /** @group setParam */
  def setIntercept(value: Boolean): this.type = set(intercept, value)

  /** @group setParam */
  def setBetaEpsilon(value: Double): this.type = set(betaEpsilon, value)

  /** @group setParam */
  def setObjectiveEpsilon(value: Double): this.type = set(objectiveEpsilon, value)

  /** @group setParam */
  def setGradientEpsilon(value: Double): this.type = set(gradientEpsilon, value)

  /** @group setParam */
  def setObjReg(value: Double): this.type = set(objReg, value)

  /** @group setParam */
  def setComputePValues(value: Boolean): this.type = set(computePValues, value)

  /** @group setParam */
  def setRemoteCollinearColumns(value: Boolean): this.type = set(removeCollinearColumns, value)

  /** @group setParam */
  def setInteractions(value: Array[String]): this.type = set(interactions, value)

  /** @group setParam */
  def setInteractionPairs(value: Array[(String, String)]): this.type = set(interactionPairs, value)

  /** @group setParam */
  def setEarlyStopping(value: Boolean): this.type = set(earlyStopping, value)


  override def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._standardize = $(standardize)
    parameters._family = $(family)
    parameters._link = $(link)
    parameters._solver = $(solver)
    parameters._tweedie_variance_power = $(tweedieVariancePower)
    parameters._tweedie_link_power = $(tweedieLinkPower)
    parameters._alpha = $(alpha)
    parameters._lambda = $(lambda_)
    parameters._missing_values_handling = $(missingValuesHandling)
    parameters._prior = $(prior)
    parameters._lambda_search = $(lambdaSearch)
    parameters._nlambdas = $(nlambdas)
    parameters._non_negative = $(nonNegative)
    parameters._exactLambdas = $(exactLambdas)
    parameters._lambda_min_ratio = $(lambdaMinRatio)
    parameters._max_iterations = $(maxIterations)
    parameters._intercept = $(intercept)
    parameters._beta_epsilon = $(betaEpsilon)
    parameters._objective_epsilon = $(objectiveEpsilon)
    parameters._gradient_epsilon = $(gradientEpsilon)
    parameters._obj_reg = $(objReg)
    parameters._compute_p_values = $(computePValues)
    parameters._remove_collinear_columns = $(removeCollinearColumns)
    parameters._interactions = $(interactions)
    parameters._interaction_pairs = {
      val pairs = $ {
        interactionPairs
      }
      if (pairs == null) {
        null
      } else {
        pairs.map(v => new StringPair(v._1, v._2))
      }
    }
    parameters._early_stopping = $(earlyStopping)
  }
}

class H2OGLMFamilyParam private[h2o](parent: Params, name: String, doc: String,
                                     isValid: Family => Boolean)
  extends EnumParam[Family](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class H2OGLMLinkParam private[h2o](parent: Params, name: String, doc: String,
                                   isValid: Link => Boolean)
  extends EnumParam[Link](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class H2OGLMSolverParam private[h2o](parent: Params, name: String, doc: String,
                                     isValid: Solver => Boolean)
  extends EnumParam[Solver](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class H2OGLMMissingValuesHandlingParam private[h2o](parent: Params, name: String, doc: String,
                                                    isValid: MissingValuesHandling => Boolean)
  extends EnumParam[MissingValuesHandling](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class H2OGLMStringPairArrayParam(parent: Params, name: String, doc: String, isValid: Array[(String, String)] => Boolean)
  extends Param[Array[(String, String)]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def jsonEncode(value: Array[(String, String)]): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      val ab = new AutoBuffer()
      ab.putASer(value.asInstanceOf[Array[AnyRef]])
      val bytes = ab.buf()
      JArray(bytes.toSeq.map(JInt(_)).toList)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): Array[(String, String)] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        val bytes = values.map {
          case JInt(x) =>
            x.byteValue()
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to Byte.")
        }.toArray
        val ab = new AutoBuffer(bytes)
        ab.getASer[(String, String)](classOf[(String, String)])
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[(String, String)].")
    }
  }
}
