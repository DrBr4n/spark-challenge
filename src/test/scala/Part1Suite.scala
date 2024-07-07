import org.opensource.sparkchallenge.App
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class Part1Suite extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Part1Suite")
    .getOrCreate()

  import spark.implicits._

  test ("An nan value should be replaced by 0.0") {

    val inputDf = Seq(
      ("Foo", "0.5"),
      ("Foo", "nan"),
      ("Foo", "1.0"),
      ("Bar", "nan"),
      ("Bar", "nan"))
      .toDF("App", "Sentiment_Polarity")

    val expected = Seq(
      ("Foo", "0.5"),
      ("Bar", "0.0"))
      .toDF("App", "Average_Sentiment_Polarity")

    val df_1 = App.part1(inputDf)

    assertResult(expected.collect())(df_1.collect())

    spark.stop()
  }
}