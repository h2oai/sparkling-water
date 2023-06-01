package ai.h2o.sparkling.repl

/**
  * Runtime patch utilities.
  */
private[repl] object PatchUtils {

  // The patcher accepts object and its defining class and return true if patching was successful
  type Patcher = (AnyRef, Class[_]) => Boolean

  // Actual patch definition
  type Patch = (ClassLoader) => Boolean

  /**
    * Path given object.
    *
    * @param fullClassName class name of object
    * @param classloader   classloader to use for loading the object definition
    * @param patcher       actual patcher
    * @return true if patching was successful else false
    */
  def patchObject(fullClassName: String, classloader: ClassLoader, patcher: Patcher): Boolean = {
    val clz = Class.forName(fullClassName + "$", false, classloader)
    val module = getModule(clz)

    // Patch it
    patcher(module, clz)
  }

  def getModule(objectClass: Class[_]): AnyRef = {
    val f = objectClass.getField("MODULE$")
    f.get(null)
  }

  val OUTER_SCOPES_CLASS = "org.apache.spark.sql.catalyst.encoders.OuterScopes"
  val OUTER_SCOPE_REPL_REGEX = """^((?:intp_id_\d+)??\$line(?:\d+)\.\$read)(?:\$\$iw)+$""".r

  // Patch Spark OuterScopes definition
  val patchOuterScopes: Patch = (classLoader: ClassLoader) => {
    val patcher: Patcher = (obj: AnyRef, clz: Class[_]) => {
      val f = clz.getDeclaredField("REPLClass")
      f.setAccessible(true)
      try {
        f.set(obj, OUTER_SCOPE_REPL_REGEX)
      } catch {
        case _: IllegalArgumentException => // we have already patched once
      }
      true
    }

    patchObject(OUTER_SCOPES_CLASS, classLoader, patcher)
  }

  // Manages all runtime patches in the system
  // Note: if necessary it should accept environment configuration and
  // apply patch only if it is applicable for given environment (e.g., Specific Scala + Specific Spark)
  object PatchManager {

    private val patches = Map(
      "SW-386" ->
        ("Patches OuterScope to replace default REPL regexp by one which understand H2O REPL", patchOuterScopes))

    def patch(jiraId: String, classLoader: ClassLoader): Boolean = {
      patches.get(jiraId).map(p => p._2(classLoader)).getOrElse(false)
    }

    def patchInfo(jiraId: String): String = {
      patches.get(jiraId).map(_._1).getOrElse("NOT FOUND")
    }
  }

}
