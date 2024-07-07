import org.opensource.sparkchallenge.App
import org.scalatest.funsuite.AnyFunSuite

class Part2Suite extends AnyFunSuite with SharedSparkSessionHelper {

  import sqlImplicits._

  test ("df_2 should have every app with rating => 4.0 by descending order") {

    val inputDf = Seq(
      ("Foo", "4.2"),
      ("Foo2", "3.0"),
      ("Bar", "4.0"),
      ("Bar2", "5.0"))
      .toDF("App", "Rating")

    val expected = Seq(
      ("Bar2", 5.0),
      ("Foo", 4.2),
      ("Bar", 4.0))
      .toDF("App", "Rating")

    val df_2 = App.part2(inputDf)

    assertResult(expected.collect())(df_2.collect())
  }
}