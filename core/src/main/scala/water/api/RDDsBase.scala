package water.api

/**
 * Generic implementation endpoint for all RDD queries.
 */
class RDDsBase[I <: RDDs, S <: RDDsBase[I,S]] extends Schema[I,S] {
  @API(help = "List of RDDs", direction = API.Direction.OUTPUT)
  val rdds: Array[RDDV3] = null
}
