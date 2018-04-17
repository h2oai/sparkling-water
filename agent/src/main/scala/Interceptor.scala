import net.bytebuddy.implementation.bind.annotation.{AllArguments, RuntimeType}
import org.apache.spark.internal.Logging


class Interceptor extends Logging {

  @RuntimeType
  def intercept(@AllArguments allArguments: Array[AnyRef]): Any = {
    logError("Unseen Categorical: " + allArguments(1))
    this
  }
}
