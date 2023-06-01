package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param._

trait ParameterConstructorMethods extends Params {

  protected def booleanParam(name: String, doc: String): BooleanParam = {
    new BooleanParam(this, name, doc)
  }

  protected def intParam(name: String, doc: String): IntParam = {
    new IntParam(this, name, doc)
  }

  protected def longParam(name: String, doc: String): LongParam = {
    new LongParam(this, name, doc)
  }

  protected def floatParam(name: String, doc: String): FloatParam = {
    new FloatParam(this, name, doc)
  }

  protected def doubleParam(name: String, doc: String): DoubleParam = {
    new DoubleParam(this, name, doc)
  }

  protected def param[T](name: String, doc: String): Param[T] = {
    new Param[T](this, name, doc)
  }

  protected def stringParam(name: String, doc: String): Param[String] = {
    new Param[String](this, name, doc)
  }

  protected def nullableStringParam(name: String, doc: String): NullableStringParam = {
    new NullableStringParam(this, name, doc)
  }

  protected def stringArrayParam(name: String, doc: String): StringArrayParam = {
    new StringArrayParam(this, name, doc)
  }

  protected def intArrayParam(name: String, doc: String): IntArrayParam = {
    new IntArrayParam(this, name, doc)
  }

  protected def doubleArrayParam(name: String, doc: String): DoubleArrayParam = {
    new DoubleArrayParam(this, name, doc)
  }

  protected def nullableDoubleArrayArrayParam(name: String, doc: String): NullableDoubleArrayArrayParam = {
    new NullableDoubleArrayArrayParam(this, name, doc)
  }

  protected def nullableBooleanArrayParam(name: String, doc: String): NullableBooleanArrayParam = {
    new NullableBooleanArrayParam(this, name, doc)
  }

  protected def nullableIntArrayParam(name: String, doc: String): NullableIntArrayParam = {
    new NullableIntArrayParam(this, name, doc)
  }

  protected def nullableFloatArrayParam(name: String, doc: String): NullableFloatArrayParam = {
    new NullableFloatArrayParam(this, name, doc)
  }

  protected def nullableDoubleArrayParam(name: String, doc: String): NullableDoubleArrayParam = {
    new NullableDoubleArrayParam(this, name, doc)
  }

  protected def nullableStringArrayParam(name: String, doc: String): NullableStringArrayParam = {
    new NullableStringArrayParam(this, name, doc)
  }

  protected def nullableStringPairArrayParam(name: String, doc: String): NullableStringPairArrayParam = {
    new NullableStringPairArrayParam(this, name, doc)
  }

  protected def nullableStringArrayArrayParam(name: String, doc: String): NullableStringArrayArrayParam = {
    new NullableStringArrayArrayParam(this, name, doc)
  }
}
