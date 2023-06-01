package ai.h2o.sparkling

import ai.h2o.sparkling.backend.external.ExternalBackendConf
import ai.h2o.sparkling.backend.internal.InternalBackendConf
import ai.h2o.sparkling.repl.H2OInterpreter
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.SparkConf
import org.apache.spark.expose.Logging

import java.io.{File, FileWriter}

/**
  * Configuration holder which is representing
  * properties passed from user to Sparkling Water.
  */
class H2OConf(val sparkConf: SparkConf)
  extends Logging
  with InternalBackendConf
  with ExternalBackendConf
  with Serializable {

  def this() = this(SparkSessionUtils.active.sparkContext.getConf)

  H2OConf.checkDeprecatedOptions(sparkConf)
  // Precondition
  require(sparkConf != null, "Spark conf was null")

  /** Copy this object */
  override def clone: H2OConf = {
    val conf = new H2OConf(sparkConf)
    conf.generatedCredentials = this.generatedCredentials
    conf.setAll(getAll)
    conf
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): H2OConf = {
    sparkConf.set(key, value)
    this
  }

  def set(key: String, value: Boolean): H2OConf = {
    sparkConf.set(key, value.toString)
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): H2OConf = {
    sparkConf.remove(key)
    this
  }

  def contains(key: String): Boolean = sparkConf.contains(key)

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = sparkConf.get(key)

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = sparkConf.get(key, defaultValue)

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = sparkConf.getOption(key)

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    sparkConf.getAll
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): H2OConf = {
    sparkConf.setAll(settings.toIterable)
    this
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = sparkConf.getInt(key, defaultValue)

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = sparkConf.getLong(key, defaultValue)

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = sparkConf.getDouble(key, defaultValue)

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = sparkConf.getBoolean(key, defaultValue)

  override def toString: String = {
    if (runsInExternalClusterMode) {
      externalConfString
    } else {
      internalConfString
    }
  }

  def getScheme(): String = {
    if (jks.isDefined && jksPass.isDefined) {
      "https"
    } else {
      "http"
    }
  }

  /** Credentials used for the communication between proxy and h2o leader node. */
  private[sparkling] def getCredentials(): Option[H2OCredentials] = {
    if (hashLogin) {
      if (this.userName.isEmpty) {
        throw new IllegalStateException(
          "Hash login is enabled, but the configuration property 'spark.ext.h2o.user.name' is not set.")
      }
      if (this.password.isEmpty) {
        throw new IllegalStateException(
          "Hash login is enabled, but the configuration property 'spark.ext.h2o.password' is not set.")
      }
    }

    if (this.proxyLoginOnly) {
      Some(resolveGeneratedCredentials())
    } else if (this.userName.isDefined && this.password.isDefined) {
      val username = this.userName.get
      val password = this.password.get
      Some(new H2OCredentials(username, password))
    } else {
      None
    }
  }

  private var generatedCredentials: H2OCredentials = null

  private var generatedLoginConfFile: String = null

  private[sparkling] def getGeneratedLoginConfFile(): Option[String] = Option(generatedLoginConfFile)

  def resolveGeneratedCredentials(): H2OCredentials = {
    if (generatedCredentials == null) {
      val username = System.getProperty("user.name")
      val password = water.network.SecurityUtils.passwordGenerator(16)
      generatedCredentials = new H2OCredentials(username, password)
      val content = generatedCredentials.toHashFileEntry()
      val tmpFile = File.createTempFile("sparkling-water-", "-hash-login.conf")
      tmpFile.deleteOnExit()
      val writer = new FileWriter(tmpFile);
      writer.write(content)
      writer.flush()
      writer.close()
      generatedLoginConfFile = tmpFile.getAbsolutePath
      SparkSessionUtils.active.sparkContext.addFile(generatedLoginConfFile)
    }
    generatedCredentials
  }
}

object H2OConf extends Logging {
  def apply(): H2OConf = new H2OConf()

  def apply(sparkConf: SparkConf): H2OConf = new H2OConf(sparkConf)

  private val deprecatedOptions = Map[String, String]()

  private def checkDeprecatedOptions(sparkConf: SparkConf): Unit = {
    deprecatedOptions.foreach {
      case (deprecated, current) =>
        val deprecatedValue = sparkConf.getOption(deprecated)
        if (deprecatedValue.isDefined) {
          val currentValue = sparkConf.getOption(current)
          if (currentValue.isDefined) {
            logWarning(
              s"Both options '$deprecated' and '$current' are specified. " +
                s"Using value '${currentValue.get}' of '$current' as the later one is deprecated.")
          } else {
            logWarning(
              s"Please use '$current' as '$deprecated' is deprecated. Passing the value '${deprecatedValue.get}' to '$current'.")
            sparkConf.set(current, deprecatedValue.get)
          }
        }
    }
  }

  private var _sparkConfChecked = false

  def sparkConfChecked = _sparkConfChecked

  def checkSparkConf(sparkConf: SparkConf): SparkConf = {
    _sparkConfChecked = true
    sparkConf.set("spark.repl.class.outputDir", H2OInterpreter.classOutputDirectory.getAbsolutePath)
    sparkConf
  }

  private[sparkling] type Doc = String
  private[sparkling] type Setter = String
  private[sparkling] type StringOption = (String, String, Setter, Doc)
  private[sparkling] type IntOption = (String, Int, Setter, Doc)
  private[sparkling] type OptionOption = (String, None.type, Setter, Doc)
  private[sparkling] type BooleanOption = (String, Boolean, Setter, Doc)
}
