package ai.h2o.sparkling.api.generation.common

import water.api.API

trait OutputResolver {
  def resolveOutputs(outputSubstitutionContext: ModelOutputSubstitutionContext): Seq[Parameter] = {
    val h2oSchemaClass = outputSubstitutionContext.h2oSchemaClass

    val outputs = h2oSchemaClass.getFields
      .filterNot(_.getAnnotation(classOf[API]) == null)
      .map { field =>
        Parameter(
          ParameterNameConverter.convertFromH2OToSW(field.getName),
          field.getName,
          if (field.getType.isPrimitive) 0 else null,
          field.getType,
          field.getAnnotation(classOf[API]).help())
      }
    outputs
  }
}
