package MapReduce

import org.scalatest.funsuite.AnyFunSuite

import HelperUtils.ObtainConfigReference

class TimeIntervalsWithMostErrorMessagesSortedTest extends AnyFunSuite {

  // Initializing configuration for testing
  val config = ObtainConfigReference("Testing.TimeIntervalsWithMostErrorMessagesSorted") match {
    case Some(value) => value
    case None => throw new RuntimeException("Could not find the config data!")
  }

  test("Test: TimeIntervalsWithMostErrorMessagesSorted -> divideTimeIntervals()") {
    val logTime = config.getString("Testing.TimeIntervalsWithMostErrorMessagesSorted.logTime")
    val expectedLogTime = config.getString("Testing.TimeIntervalsWithMostErrorMessagesSorted.expectedLogTime")
    val timeInterval = TimeIntervalsWithMostErrorMessagesSorted.divideTimeIntervals(logTime)
    assert(timeInterval == expectedLogTime)
  }

  test("Test: TimeIntervalsWithMostErrorMessagesSorted -> run()") {
    intercept[Throwable] {
      TimeIntervalsWithMostErrorMessagesSorted.run(Array("input/", "output/"))
    }
  }
}

