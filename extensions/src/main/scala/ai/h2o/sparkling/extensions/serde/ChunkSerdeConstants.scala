package ai.h2o.sparkling.extensions.serde

trait ChunkSerdeConstants {

  /**
    * Meta Information used to specify whether we should expect sparse or dense vector
    */
  val VECTOR_IS_SPARSE = true
  val VECTOR_IS_DENSE = false

  /**
    * This is used to inform us that another byte is coming.
    * That byte can be either {@code MARKER_ORIGINAL_VALUE} or {@code MARKER_NA}. If it's
    * {@code MARKER_ORIGINAL_VALUE}, that means
    * the value sent is in the previous data sent, otherwise the value is NA.
    */
  val NUM_MARKER_NEXT_BYTE_FOLLOWS: Byte = 127

  /**
    * Same as above, but for Strings. We are using unicode code for CONTROL, which should be very very rare
    * String to send as usual String data.
    */
  val STR_MARKER_NEXT_BYTE_FOLLOWS = "\u0080"

  /**
    * Marker informing us that the data are not NA and are stored in the previous byte
    */
  val MARKER_ORIGINAL_VALUE = 0

  /**
    * Marker informing us that the data being sent is NA
    */
  val MARKER_NA = 1
}

object ChunkSerdeConstants extends ChunkSerdeConstants
