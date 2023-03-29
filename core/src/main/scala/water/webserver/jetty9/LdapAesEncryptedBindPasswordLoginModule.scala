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

package water.webserver.jetty9

import org.apache.spark.sql.SparkSession

import java.nio.charset.StandardCharsets
import java.util
import java.util.Base64
import javax.crypto._
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import javax.security.auth.Subject
import javax.security.auth.callback.CallbackHandler
import javax.xml.bind.DatatypeConverter
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A custom Jetty login module which takes AES encrypted password from the config file, and decrypts it using AES CBC Key & IV provided through Spark config.
  * Created for a specific use case where the end user doesn't have access to Spark Session, but has access to LDAP config file.
  * When no IV is provided an all zero IV is used.
  *
  * Example password encryption command:
  * openssl aes-256-cbc -in file.in -out file.out -iv 064df9633d9f5dd0b5614843f6b4b059 -K b38b730d4cc721156e3760d1d58546ce697adc939188e4c6a80f0e24e032b9b7 -base64 -nosalt
  */
class LdapAesEncryptedBindPasswordLoginModule extends org.eclipse.jetty.jaas.spi.LdapLoginModule {

  private val Algo = "AES"
  private val AlgoMode = "CBC"
  private val PaddingMode = "PKCS5Padding"
  private val EncryptedBindPasswordPropertyName = "encryptedBindPassword"
  private val BindPasswordPropertyName = "bindPassword"
  private val AesKeyPropertyName = "spark.ext.h2o.jetty.aes.login.module.key"
  private val AesIvPropertyName = "spark.ext.h2o.jetty.aes.login.module.iv"

  override def initialize(
      subject: Subject,
      callbackHandler: CallbackHandler,
      sharedState: util.Map[String, _],
      javaOptions: util.Map[String, _]): Unit = {
    val options = mutable.Map(javaOptions.asScala.asInstanceOf[mutable.Map[String, AnyRef]].toSeq: _*)
    if (options.isDefinedAt(BindPasswordPropertyName)) {
      throw new IllegalArgumentException(
        s"$BindPasswordPropertyName option not supported, please use $EncryptedBindPasswordPropertyName")
    }

    val spark = SparkSession.getActiveSession
      .getOrElse(throw new IllegalStateException("No Spark Session available!"))
    val inputKey = spark.conf
      .getOption(AesKeyPropertyName)
      .map(_.trim)
      .getOrElse(throw new IllegalStateException(s"$AesKeyPropertyName must be set"))
    val inputIV = spark.conf
      .getOption(AesIvPropertyName)
      .map(_.trim)
      .getOrElse("0" * 32)

    options
      .remove(EncryptedBindPasswordPropertyName)
      .map(_.asInstanceOf[String].trim)
      .foreach { pwd =>
        options.put(BindPasswordPropertyName, decryptPassword(pwd, inputKey, inputIV))
      }
    super.initialize(subject, callbackHandler, sharedState, options.asJava)
  }

  private def decryptPassword(encryptedPassword: String, inputKey: String, inputIV: String): String = {
    val key: Array[Byte] = DatatypeConverter.parseHexBinary(inputKey)
    val iv: Array[Byte] = DatatypeConverter.parseHexBinary(inputIV)
    val cipher = Cipher.getInstance(s"$Algo/$AlgoMode/$PaddingMode")
    val secretKey = new SecretKeySpec(key, Algo)
    cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv))
    val plainText = cipher.doFinal(Base64.getDecoder.decode(encryptedPassword))
    new String(plainText, StandardCharsets.UTF_8)
  }
}
