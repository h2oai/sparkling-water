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

package ai.h2o.sparkling.booklet.generation

import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.backend.external.ExternalBackendConf
import ai.h2o.sparkling.backend.internal.InternalBackendConf

import scala.reflect.runtime.universe._

object ConfigurationsTemplate {

  def apply(): String = {
    val sharedConfOptions = getOptions[SharedBackendConf.type](SharedBackendConf)
    val internalConfOptions = getOptions[InternalBackendConf.type](InternalBackendConf)
    val externalConfOptions = getOptions[ExternalBackendConf.type](ExternalBackendConf)

    val firstLines = "\\documentclass{standalone}\n" + """\""" + """usepackage{placeins}"""

    firstLines + """
       |\begin{document}
       |
       |	\section{Sparkling Water Configuration Properties}
       |	\label{sec:properties}
       |
       |	The following configuration properties can be passed to Spark to configure Sparkling Water:
       |
       |	\subsection{Configuration Properties Independent of Selected Backend}
       |	\begin{footnotesize}
       |		\begin{longtable}[!ht]{l p{2.0cm} p{3.0cm}}
       |			\toprule
       |			Property name & Default & Description \\
       |			\midrule
       |
       """.stripMargin + generateOptions(sharedConfOptions) + """
       |			\bottomrule
       |		\end{longtable}
       |	\end{footnotesize}
       |
       |
       |	\subsection{Internal Backend Configuration Properties}
       |	\begin{footnotesize}
       |		\begin{longtable}[!ht]{l p{2.0cm} p{3.0cm}}
       |			\toprule
       |			Property name & Default & Description \\
       |			\midrule
       |
       """.stripMargin + generateOptions(internalConfOptions) + """
       |			\bottomrule
       |		\end{longtable}
       |	\end{footnotesize}
       |
       |
       |	\subsection{External Backend Configuration Properties}
       |	\begin{footnotesize}
       |		\begin{longtable}[!ht]{l l p{3.0cm}}
       |			\toprule
       |			Property name & Default & Description \\
       |			\midrule
       |
       """.stripMargin + generateOptions(externalConfOptions) + """
       |			\bottomrule
       |		\end{longtable}
       |	\end{footnotesize}
       |
       |\end{document}""".stripMargin
  }

  case class Option(name: String, value: String, setters: String, doc: String)

  private def getOptions[T](t: Object)(implicit tag: TypeTag[T]): Array[Option] = {
    val ru = scala.reflect.runtime.universe
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val instanceMirror = rm.reflect(t)
    val typ = ru.typeOf[T]
    val members = typ.members.filter(_.isPublic).filter(_.name.toString.startsWith("PROP_"))
    val reflectedMembers = members.map(_.asTerm).map(instanceMirror.reflectField)
    reflectedMembers
      .map { member =>
        val optionTuple = member.get.asInstanceOf[(String, Any, String, String)]
        Option(optionTuple._1, optionTuple._2.toString, optionTuple._3, optionTuple._4)
      }
      .toArray
      .reverse
  }

  private def generateOptions(options: Array[Option]): String = {
    options.map(generateOption).mkString("\n")
  }

  private def generateOption(option: Option): String = {
    s"			${option.name} & ${option.value} & ${option.doc} \\\\ \\addlinespace"
      .replaceAllLiterally("_", "\\_")
      .replaceAllLiterally("#", "\\#")
      .replaceAllLiterally("<", "\\(\\langle\\)")
      .replaceAllLiterally(">", "\\(\\rangle\\)")
  }

}
