package ai.h2o.sparkling.macros

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

/**
  * The class represents an annotation specifying deprecated methods of Sparkling Water API
  *
  * @param replacement Name of a method replacing the deprecated method
  * @param version     Version when this method will be removed
  */
class DeprecatedMethod(replacement: String = "", version: String = "") extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro DeprecatedMethodMacro.impl
}

/**
  * The object contains all the logic for expanding the [[DeprecatedMethod]] annotation.
  */
object DeprecatedMethodMacro {

  /**
    * The method replaces the [[DeprecatedMethod]] annotation with the standard [[deprecated]] annotation and injects
    * a logging logic into an annotated method.
    *
    * @param c         A reflection white-box context
    * @param annottees An expression annotated by [[DeprecatedMethod]]
    * @return An expression that is result of the annotation expansion
    */
  def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    // Extract the annotation parameter from its definition
    val (replacement, version): (String, String) = c.prefix.tree match {
      case q"new DeprecatedMethod(replacement = $r)" => (c.eval[String](c.Expr(r)), "")
      case q"new DeprecatedMethod(version = $v)" => ("", c.eval[String](c.Expr(v)))
      case q"new DeprecatedMethod($r)" => (c.eval[String](c.Expr(r)), "")
      case q"new DeprecatedMethod($r, $v)" => (c.eval[String](c.Expr(r)), c.eval[String](c.Expr(v)))
      case q"new DeprecatedMethod()" => ("", "")
      case _ => c.abort(c.enclosingPosition, "Unexpected annotation pattern for DeprecatedMethod!")
    }

    // Inject scala.deprecated annotation and a logic for logging a warning.
    val result = {
      annottees.map(_.tree).toList match {
        case q"${Modifiers(f, p, a)} def $methodName[..$tpes](...$args): $returnType = { ..$body }" :: Nil => {

          val daMessage = if (replacement.isEmpty) "" else s"Use '$replacement' instead!"
          val removalMessage = if (version.isEmpty) "" else s"This method will be removed in the release $version."
          val da = q"""new deprecated($daMessage, "")"""
          val warnMessage = s"The method '$methodName' is deprecated. $daMessage $removalMessage"

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
