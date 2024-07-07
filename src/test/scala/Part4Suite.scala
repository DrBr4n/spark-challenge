import org.opensource.sparkchallenge.App
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class Part4Suite extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Part4Suite")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  import spark.implicits._

  test ("Assert that part4 joins the data frames correctly") {

    val inputDf1 = Seq(
      ("Foo", "0.5"),
      ("Bar", "0.0"))
      .toDF("App", "Average_Sentiment_Polarity")

    val categories = Array("GAMES", "FAMILY")

    val inputDf2 = Seq(
      ("Foo", categories ,"4.0", "10", "1.5", "1000+", "Paid", "$2.0", "Everyone", "Games", "January 7, 2018", "2.0.0", "4.0.3 and up"),
      ("Bar", categories ,"4.5", "20", "1.5625", "2000+", "Paid", "$6.0", "Everyone", "Family", "June 10, 2020", "2.0.0", "3.0.0 and up"))
      .toDF("App", "Categories", "Rating", "Reviews", "Size",
        "Installs", "Type", "Price", "Content_Rating", "Genres",
        "Last_Updated", "Current_Version", "Minimum_Android_Version")

    val expected = Seq(
      ("Foo", categories ,"4.0", "10", "1.5", "1000+", "Paid", "$2.0", "Everyone", "Games", "January 7, 2018", "2.0.0", "4.0.3 and up", "0.5"),
      ("Barr", categories,"4.5", "20", "1.5625", "2000+", "Paid", "$6.0", "Everyone", "Family", "June 10, 2020", "2.0.0", "3.0.0 and up", "0.0"))
      .toDF("App", "Categories", "Rating", "Reviews", "Size",
        "Installs", "Type", "Price", "Content_Rating", "Genres",
        "Last_Updated", "Current_Version", "Minimum_Android_Version", "Average_Sentiment_Polarity")

    val df_4 = App.part4(inputDf1, inputDf2)

    assertResult(expected.collect())(df_4.collect())

    spark.stop()
  }
}