import org.scalatest.funsuite.AnyFunSuite

class RunMapReduceTasksTest extends AnyFunSuite{

  test("Test: DistributionOfLogsAcrossTimeIntervals -> run()") {
    intercept[Throwable] {
      JobDriver.main(Array("input/", "output/"))
    }
  }
}
