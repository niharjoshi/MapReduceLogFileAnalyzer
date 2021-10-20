import MapReduce.*

object JobDriver {

  def main(args: Array[String]) = {
    NumberOfMessagesForEachLogType.run(args)
    DistributionOfLogsAcrossTimeIntervals.run(args)
    TimeIntervalsWithMostErrorMessagesSorted.run(args)
    HighestNumberOfCharactersForEachLogType.run(args)
    System.exit(1)
  }
}