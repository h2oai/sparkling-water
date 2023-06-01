package ai.h2o.sparkling.extensions.rest.api

object Paths {
  val CHUNK: String = "/3/Chunk"
  val CHUNK_CATEGORICAL_DOMAINS: String = "/3/ChunkCategoricalDomains"
  val INITIALIZE_FRAME: String = "/3/InitializeFrame"
  val FINALIZE_FRAME: String = "/3/FinalizeFrame"
  val UPLOAD_PLAN: String = "/3/UploadPlan"
  val LOG_LEVEL: String = "/3/LogLevel"
  val SPARKLING_INTERNAL: String = "/3/SparklingInternal/*"
  val SW_AVAILABLE: String = "/3/scalaint" // H2O Flow determines whether we run Sparkling Water
  // based on existence of /3/scalaint endpoint. We therefore expose dummy endpoint on H2O-3 side
  // which ensures that H2O Flow expose additional Sparkling Water related buttons
  val VERIFY_WEB_OPEN: String = "/3/verifyWebOpen"
  val VERIFY_VERSION: String = "/3/verifyVersion"
}
