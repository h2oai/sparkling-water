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

import ai.h2o.sparkling.macros.DeprecatedMethod
import hex.StringPair
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.MissingValuesHandling
import hex.glm.GLMModel.GLMParameters
import hex.glm.GLMModel.GLMParameters.{Family, Link, Solver}
import hex.glm.{GLM, GLMModel}
import hex.schemas.GLMV3.GLMParametersV3
import org.apache.spark.ml.h2o.param.H2OAlgoParams
import org.apache.spark.ml.h2o.param.H2OAlgoParamsHelper.getValidatedEnumValue
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JValue}
import water.AutoBuffer

/**
  * H2O GLM algorithm exposed via Spark ML pipelines.
  */
class H2OGLM(override val uid: String) extends H2OAlgorithm[GLM, GLMModel, GLMParameters] with H2OGLMParams {

  def this() = this(Identifiable.randomUID("glm"))
}

object H2OGLM extends DefaultParamsReadable[py_sparkling.ml.algos.H2OGLM]


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
  private val standardize = booleanParam("standardize")
  private val family = stringParam( "family", "family")
  private val link = stringParam( "link", "link")
  private val solver = stringParam( "solver", "solver")
  private val tweedieVariancePower = doubleParam("tweedieVariancePower")
  private val tweedieLinkPower = doubleParam("tweedieLinkPower")
  private val alpha = nullableDoubleArrayParam("alpha")
  private val lambda_ = nullableDoubleArrayParam("lambda_", "lambda")
  private val missingValuesHandling = stringParam(  "missingValuesHandling", "missingValuesHandling")
  private val prior = doubleParam("prior")
  private val lambdaSearch = booleanParam("lambdaSearch")
  private val nlambdas = intParam("nlambdas")
  private val nonNegative = booleanParam("nonNegative")
  private val exactLambdas = booleanParam("exactLambdas", "exact lambdas")
  private val lambdaMinRatio = doubleParam("lambdaMinRatio")
  private val maxIterations = intParam("maxIterations")
  private val intercept = booleanParam("intercept")
  private val betaEpsilon = doubleParam("betaEpsilon")
  private val objectiveEpsilon = doubleParam("objectiveEpsilon")
  private val gradientEpsilon = doubleParam("gradientEpsilon")
  private val objReg = doubleParam("objReg")
  private val computePValues = booleanParam("computePValues")
  private val removeCollinearCols = booleanParam("removeCollinearCols", "A flag indicating whether collinear columns should be removed or not")
  private val interactions = nullableStringArrayParam("interactions")
  private val interactionPairs = new H2OGLMStringPairArrayParam(this, "interactionPairs", "interactionPairs")
  private val earlyStopping = booleanParam("earlyStopping")

  //
  // Default values
  //
  setDefault(
    standardize -> true,
    family -> Family.gaussian.name(),
    link -> Link.family_default.name(),
    solver -> Solver.AUTO.name(),
    tweedieVariancePower -> 0,
    tweedieLinkPower -> 0,
    alpha -> null,
    lambda_ -> null,
    missingValuesHandling -> MissingValuesHandling.MeanImputation.name(),
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
    removeCollinearCols -> false,
    interactions -> null,
    interactionPairs -> null,
    earlyStopping -> true
  )

  //
  // Getters
  //
  def getStandardize(): Boolean = $(standardize)

  def getFamily(): String = $(family)

  def getLink(): String = $(link)

  def getSolver(): String = $(solver)

  def getTweedieVariancePower(): Double = $(tweedieVariancePower)

  def getTweedieLinkPower(): Double = $(tweedieLinkPower)

  def getAlpha(): Array[Double] = $(alpha)

  def getLambda(): Array[Double] = $(lambda_)

  def getMissingValuesHandling(): String = $(missingValuesHandling)

  def getPrior(): Double = $(prior)

  def getLambdaSearch(): Boolean = $(lambdaSearch)

  def getNlambdas(): Int = $(nlambdas)

  def getNonNegative(): Boolean = $(nonNegative)

  def getExactLambdas(): Boolean = $(exactLambdas)

  def getLambdaMinRatio(): Double = $(lambdaMinRatio)

  def getMaxIterations(): Int = $(maxIterations)

  def getIntercept(): Boolean = $(intercept)

  def getBetaEpsilon(): Double = $(betaEpsilon)

  def getObjectiveEpsilon(): Double = $(objectiveEpsilon)

  def getGradientEpsilon(): Double = $(gradientEpsilon)

  def getObjReg(): Double = $(objReg)

  def getComputePValues(): Boolean = $(computePValues)

  def getRemoveCollinearCols(): Boolean = $(removeCollinearCols)

  def getInteractions(): Array[String] = $(interactions)

  def getInteractionPairs(): Array[(String, String)] = $(interactionPairs)

  def getEarlyStopping(): Boolean = $(earlyStopping)


  //
  // Setters
  //
  def setStandardize(value: Boolean): this.type = set(standardize, value)

  @DeprecatedMethod("setFamily(value: String)")
  def setFamily(value: Family): this.type = setFamily(value.name())

  def setFamily(value: String): this.type = {
    val validated = getValidatedEnumValue[Family](value)
    set(family, validated)
  }

  @DeprecatedMethod("setLink(value: String)")
  def setLink(value: Link): this.type = setLink(value.name())

  def setLink(value: String): this.type = {
    val validated = getValidatedEnumValue[Link](value)
    set(link, validated)
  }

  @DeprecatedMethod("setSolver(value: String)")
  def setSolver(value: Solver): this.type = setSolver(value.name())

  def setSolver(value: String): this.type = {
    val validated = getValidatedEnumValue[Solver](value)
    set(solver, validated)
  }

  def setTweedieVariancePower(value: Double): this.type = set(tweedieVariancePower, value)

  def setTweedieLinkPower(value: Double): this.type = set(tweedieLinkPower, value)

  def setAlpha(value: Array[Double]): this.type = set(alpha, value)

  def setLambda(value: Array[Double]): this.type = set(lambda_, value)

  @DeprecatedMethod("setMissingValuesHandling(value: String)")
  def setMissingValuesHandling(value: MissingValuesHandling): this.type = setMissingValuesHandling(value.name())

  def setMissingValuesHandling(value: String): this.type = {
    val validated = getValidatedEnumValue[MissingValuesHandling](value)
    set(missingValuesHandling, validated)
  }

  def setPrior(value: Double): this.type = set(prior, value)

  def setLambdaSearch(value: Boolean): this.type = set(lambdaSearch, value)

  def setNlambdas(value: Int): this.type = set(nlambdas, value)

  def setNonNegative(value: Boolean): this.type = set(nonNegative, value)

  def setExactLambdas(value: Boolean): this.type = set(exactLambdas, value)

  def setLambdaMinRatio(value: Double): this.type = set(lambdaMinRatio, value)

  def setMaxIterations(value: Int): this.type = set(maxIterations, value)

  def setIntercept(value: Boolean): this.type = set(intercept, value)

  def setBetaEpsilon(value: Double): this.type = set(betaEpsilon, value)

  def setObjectiveEpsilon(value: Double): this.type = set(objectiveEpsilon, value)

  def setGradientEpsilon(value: Double): this.type = set(gradientEpsilon, value)

  def setObjReg(value: Double): this.type = set(objReg, value)

  def setComputePValues(value: Boolean): this.type = set(computePValues, value)

  def setRemoveCollinearCols(value: Boolean): this.type = set(removeCollinearCols, value)

  def setInteractions(value: Array[String]): this.type = set(interactions, value)

  def setInteractionPairs(value: Array[(String, String)]): this.type = set(interactionPairs, value)

  def setEarlyStopping(value: Boolean): this.type = set(earlyStopping, value)


  override def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._standardize = $(standardize)
    parameters._family = Family.valueOf($(family))
    parameters._link = Link.valueOf($(link))
    parameters._solver = Solver.valueOf($(solver))
    parameters._tweedie_variance_power = $(tweedieVariancePower)
    parameters._tweedie_link_power = $(tweedieLinkPower)
    parameters._alpha = $(alpha)
    parameters._lambda = $(lambda_)
    parameters._missing_values_handling = MissingValuesHandling.valueOf($(missingValuesHandling))
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
    parameters._remove_collinear_columns = $(removeCollinearCols)
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
