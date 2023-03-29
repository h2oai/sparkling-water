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
import java.util
import java.util.Base64
import javax.security.auth.Subject
import javax.security.auth.callback.CallbackHandler
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * An adapter for org.eclipse.jetty.jaas.spi.LdapLoginModule taking base64 password instead of plain text.
  */
class LdapBase64LoginModule extends org.eclipse.jetty.jaas.spi.LdapLoginModule {

  private val Base64PasswordPropertyName = "base64BindPassword"
  private val BindPasswordPropertyName = "bindPassword"

  override def initialize(
      subject: Subject,
      callbackHandler: CallbackHandler,
      sharedState: util.Map[String, _],
      javaOptions: util.Map[String, _]): Unit = {
    val options = collection.mutable.Map(javaOptions.asScala.asInstanceOf[mutable.Map[String, AnyRef]].toSeq: _*)
    if (options.isDefinedAt(BindPasswordPropertyName)) {
      throw new IllegalArgumentException(
        s"$BindPasswordPropertyName option not supported, please use $Base64PasswordPropertyName")
    }

    options
      .remove(Base64PasswordPropertyName)
      .map(_.asInstanceOf[String])
      .foreach { pwd =>
        options.put(BindPasswordPropertyName, decodeBase64(pwd))
      }
    super.initialize(subject, callbackHandler, sharedState, options.asJava)
  }

  private def decodeBase64(input: String): String = new String(Base64.getDecoder.decode(input))
}
