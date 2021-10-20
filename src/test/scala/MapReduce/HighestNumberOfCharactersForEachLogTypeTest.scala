package MapReduce

import org.scalatest.funsuite.AnyFunSuite

import HelperUtils.ObtainConfigReference

class HighestNumberOfCharactersForEachLogTypeTest extends AnyFunSuite {

  test("Test: HighestNumberOfCharactersForEachLogType -> run()") {
    intercept[Throwable] {
      HighestNumberOfCharactersForEachLogType.run(Array("input/", "output/"))
    }
  }
}
