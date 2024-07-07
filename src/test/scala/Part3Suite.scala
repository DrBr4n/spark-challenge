import org.opensource.sparkchallenge.App
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class Part3Suite extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Part3Suite")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  import spark.implicits._

  test ("Assert if df_3 produces expected data frame") {

    val inputDf = Seq(
      ("Foo", "GAMES" ,"4.0", "10", "1.5M", "1000+", "Paid", "$2.0", "Everyone", "Games", "January 7, 2018", "2.0.0", "4.0.3 and up"),
      ("Foo", "FAMILY" ,"4.5", "20", "1600k", "1000+", "Paid", "$4.0", "Everyone", "Family", "January 10, 2019", "3.0.0", "4.0.0 and up"))
      .toDF("App", "Category", "Rating", "Reviews", "Size",
        "Installs", "Type", "Price", "Content Rating", "Genres",
        "Last Updated", "Current Ver", "Android Ver")

    val categories = Array("GAMES", "FAMILY")

    val expected = Seq(
    ("Foo", categories, "4.5", "20", "1.5625", "1000+", "Free", "0.0", "Everyone", "Family", "January 10, 2019", "3.0.0", "4.0.3 and up"))
      .toDF("App", "Category", "Rating", "Reviews", "Size",
        "Installs", "Type", "Price", "Content_Rating", "Genres",
        "Last_Updated", "Current_Version", "Minimum_Android_Version")

    val df_3 = App.part3(inputDf)

    assertResult(expected.collect())(df_3.collect())

    spark.stop()
  }
}
