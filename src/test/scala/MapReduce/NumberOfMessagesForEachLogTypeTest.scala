package MapReduce

import org.scalatest.funsuite.AnyFunSuite

import HelperUtils.ObtainConfigReference

class NumberOfMessagesForEachLogTypeTest extends AnyFunSuite {

  test("Test: NumberOfMessagesForEachLogType -> run()") {
    intercept[Throwable] {
      NumberOfMessagesForEachLogType.run(Array("input/", "output/"))
    }
  }
}

