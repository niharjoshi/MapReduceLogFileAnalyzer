package MapReduce

import org.scalatest.funsuite.AnyFunSuite

import HelperUtils.ObtainConfigReference

class DistributionOfLogsAcrossTimeIntervalsTest extends AnyFunSuite {

  // Initializing configuration for testing
  val config = ObtainConfigReference("Testing.DistributionOfLogsAcrossTimeIntervals") match {
    case Some(value) => value
    case None => throw new RuntimeException("Could not find the config data!")
  }

  test("Test: DistributionOfLogsAcrossTimeIntervals -> divideTimeIntervals()") {
    val logTime = config.getString("Testing.DistributionOfLogsAcrossTimeIntervals.logTime")
    val expectedLogTime = config.getString("Testing.DistributionOfLogsAcrossTimeIntervals.expectedLogTime")
    val timeInterval = DistributionOfLogsAcrossTimeIntervals.divideTimeIntervals(logTime)
    assert(timeInterval == expectedLogTime)
  }

  test("Test: DistributionOfLogsAcrossTimeIntervals -> run()") {
    intercept[Throwable] {
      DistributionOfLogsAcrossTimeIntervals.run(Array("input/", "output/"))
    }
  }
}
