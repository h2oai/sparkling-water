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

package water.util

import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

/**
  * The class represents an annotation specifying deprecated methods of Sparkling Water API
  * @param replacement Name of a method replacing the deprecated method
  */
@compileTimeOnly("enable macro paradise to expand macro annotations")
class DeprecatedMethod(replacement: String = "") extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro DeprecatedMethodMacro.impl
}

/**
  * The object contains all the logic for expanding the [[DeprecatedMethod]] annotation.
  */
object DeprecatedMethodMacro {

  /**
    * The method replaces the [[DeprecatedMethod]] annotation with the standard [[deprecated]] annotation and injects
    * a logging logic into an annotated method.
    * @param c A reflection white-box context
    * @param annottees An expression annotated by [[DeprecatedMethod]]
    * @return An expression that is result of the annotation expansion
    */
  def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    // Extract the annotation parameter from its definition
    val replacement: String = c.prefix.tree match {
      case q"new DeprecatedMethod(replacement = $r)" => c.eval[String](c.Expr(r))
      case q"new DeprecatedMethod($r)" => c.eval[String](c.Expr(r))
      case q"new DeprecatedMethod()" => ""
      case _ => c.abort(c.enclosingPosition, "Unexpected annotation pattern for DeprecatedMethod!")
    }

    // Inject scala.deprecated annotation and a logic for logging a warning.
    val result = {
      annottees.map(_.tree).toList match {
        case q"${Modifiers(f, p, a)} def $methodName[..$tpes](...$args): $returnType = { ..$body }" :: Nil => {

          val daMessage = if (replacement.isEmpty) "" else s"Use '$replacement' instead!"
          val da = q"""new deprecated($daMessage, "")"""
          val warnMessage = s"The method '$methodName' is deprecated. $daMessage"

          q"""${Modifiers(f, p, da :: a)} def $methodName[..$tpes](...$args): $returnType =  {
            logWarning($warnMessage)
            ..$body
          }"""
        }
        case _ => c.abort(c.enclosingPosition, "Annotation @DeprecatedMethod can be used only with methods")
      }
    }
    c.Expr[Any](result)
  }
}
