package water.fvec

/**
 * Simple bring to access package-private members.
 */
object FrameUtils {

  def preparePartialFrame(fr: Frame, names: Array[String]): Unit = {
    fr.preparePartialFrame(names)
  }

  def finalizePartialFrame(fr: Frame, rowsPerChunk: Array[Long],
                           colDomains: Array[Array[String]],
                           colTypes: Array[Byte]): Unit = {
    fr.finalizePartialFrame(rowsPerChunk, colDomains, colTypes)
  }

  def createNewChunks( name: String, cidx: Int ): Array[NewChunk] = Frame.createNewChunks(name, cidx)

  def closeNewChunks( nchks : Array[NewChunk] ): Unit = Frame.closeNewChunks(nchks)
}
