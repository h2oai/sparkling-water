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
package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.params.H2OAlgoParamsHelper._
import ai.h2o.sparkling.ml.params.H2OAlgoSupervisedParams
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import hex.glm.GLMModel.GLMParameters
import hex.glm.GLMModel.GLMParameters.{Family, Link, MissingValuesHandling, Solver}
import hex.schemas.GLMV3.GLMParametersV3
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.ml.util.Identifiable
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JValue}
import water.AutoBuffer

/**
  * H2O GLM algorithm exposed via Spark ML pipelines.
  */
class H2OGLM(override val uid: String) extends H2OSupervisedAlgorithm[GLMParameters] with H2OGLMParams {

  def this() = this(Identifiable.randomUID(classOf[H2OGLM].getSimpleName))
}

object H2OGLM extends H2OParamsReadable[H2OGLM]


/**
  * Parameters for Spark's API exposing underlying H2O model.
  */
trait H2OGLMParams extends H2OAlgoSupervisedParams[GLMParameters] {

  type H2O_SCHEMA = GLMParametersV3

  protected def paramTag = reflect.classTag[GLMParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //
  private val standardize = booleanParam("standardize")
  private val family = stringParam("family", "family")
  private val link = stringParam("link", "link")
  private val solver = stringParam("solver", "solver")
  private val tweedieVariancePower = doubleParam("tweedieVariancePower")
  private val tweedieLinkPower = doubleParam("tweedieLinkPower")
  private val alphaValue = nullableDoubleArrayParam("alphaValue", "alpha values")
  private val lambdaValue = nullableDoubleArrayParam("lambdaValue", "lambda values")
  private val missingValuesHandling = stringParam("missingValuesHandling", "missingValuesHandling")
  private val prior = doubleParam("prior")
  private val lambdaSearch = booleanParam("lambdaSearch")
  private val nlambdas = intParam("nlambdas")
  private val nonNegative = booleanParam("nonNegative")
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
    alphaValue -> null,
    lambdaValue -> null,
    missingValuesHandling -> MissingValuesHandling.MeanImputation.name(),
    prior -> -1,
    lambdaSearch -> false,
    nlambdas -> -1,
    nonNegative -> false,
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

  def getAlphaValue(): Array[Double] = $(alphaValue)

  def getLambdaValue(): Array[Double] = $(lambdaValue)

  def getMissingValuesHandling(): String = $(missingValuesHandling)

  def getPrior(): Double = $(prior)

  def getLambdaSearch(): Boolean = $(lambdaSearch)

  def getNlambdas(): Int = $(nlambdas)

  def getNonNegative(): Boolean = $(nonNegative)

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

  def setFamily(value: String): this.type = {
    val validated = getValidatedEnumValue[Family](value)
    set(family, validated)
  }

  def setLink(value: String): this.type = {
    val validated = getValidatedEnumValue[Link](value)
    set(link, validated)
  }

  def setSolver(value: String): this.type = {
    val validated = getValidatedEnumValue[Solver](value)
    set(solver, validated)
  }

  def setTweedieVariancePower(value: Double): this.type = set(tweedieVariancePower, value)

  def setTweedieLinkPower(value: Double): this.type = set(tweedieLinkPower, value)

  def setAlphaValue(value: Array[Double]): this.type = set(alphaValue, value)

  def setLambdaValue(value: Array[Double]): this.type = set(lambdaValue, value)

  def setMissingValuesHandling(value: String): this.type = {
    val validated = getValidatedEnumValue[MissingValuesHandling](value)
    set(missingValuesHandling, validated)
  }

  def setPrior(value: Double): this.type = set(prior, value)

  def setLambdaSearch(value: Boolean): this.type = set(lambdaSearch, value)

  def setNlambdas(value: Int): this.type = set(nlambdas, value)

  def setNonNegative(value: Boolean): this.type = set(nonNegative, value)

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

  override private[sparkling] def getH2OAlgorithmParams(): Map[String, Any] = {
    super.getH2OAlgorithmParams() ++
      Map(
        "standardize" -> getStandardize(),
        "family" -> getFamily(),
        "link" -> getLink(),
        "solver" -> getSolver(),
        "tweedie_variance_power" -> getTweedieVariancePower(),
        "tweedie_link_power" -> getTweedieLinkPower(),
        "alpha" -> getAlphaValue(),
        "lambda" -> getLambdaValue(),
        "missing_values_handling" -> getMissingValuesHandling(),
        "prior" -> getPrior(),
        "lambda_search" -> getLambdaSearch(),
        "nlambdas" -> getNlambdas(),
        "non_negative" -> getNonNegative(),
        "lambda_min_ratio" -> getLambdaMinRatio(),
        "max_iterations" -> getMaxIterations(),
        "intercept" -> getIntercept(),
        "beta_epsilon" -> getBetaEpsilon(),
        "objective_epsilon" -> getObjectiveEpsilon(),
        "gradient_epsilon" -> getGradientEpsilon(),
        "obj_reg" -> getObjReg(),
        "compute_p_values" -> getComputePValues(),
        "remove_collinear_columns" -> getRemoveCollinearCols(),
        "interactions" -> getInteractions(),
        "interaction_pairs" -> {
          val pairs = getInteractionPairs()
          if (pairs == null) null else pairs.toMap
        },
        "early_stopping" -> getEarlyStopping()
      )
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
