package ai.h2o.sparkling.backend.utils

object ProgressBar {
  private final val BarLength = 50
  private final val CarriageReturn = "\r"
  private final val Empty = "_"
  private final val Filled = "█"
  private final val Side = "|"

  private[sparkling] def printProgressBar(progress: Float, leaveTheProgressBarVisible: Boolean = false): Unit = {
    val crOrNewline = if (leaveTheProgressBarVisible) System.lineSeparator else CarriageReturn
    System.err.print(CarriageReturn + renderProgressBar(progress) + crOrNewline)
  }

  private[sparkling] def renderProgressBar(progress: Float): String = {
    val filledPartLength = (progress * BarLength).ceil.toInt
    val filledPart = Filled * filledPartLength
    val emptyPart = Empty * (BarLength - filledPartLength)
    val progressPercent = (progress * 100).toInt
    val percentagePart = s" $progressPercent%"
    Side + filledPart + emptyPart + Side + percentagePart
  }

}
