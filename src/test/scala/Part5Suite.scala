import org.opensource.sparkchallenge.App
import org.scalatest.funsuite.AnyFunSuite

class Part5Suite extends AnyFunSuite with SharedSparkSessionHelper{

  import sqlImplicits._

  test ("Assert that part5 joins the data frames correctly and produces the expected result") {

    val inputDf1 = Seq(
      ("Foo", "0.5"),
      ("Bar", "0.0"))
      .toDF("App", "Average_Sentiment_Polarity")

    val inputDf2 = Seq(
      ("Foo", "Games" ,"4.0", "10", "1.5", "1000+", "Paid", "$2.0", "Everyone", "Games", "January 7, 2018", "2.0.0", "4.0.3 and up"),
      ("Bar", "Family" ,"4.5", "20", "1.5625", "2000+", "Paid", "$6.0", "Everyone", "Games", "June 10, 2020", "2.0.0", "3.0.0 and up"),
      ("Bar", "Art" ,"3.0", "20", "1.5625", "2000+", "Paid", "$8.0", "Everyone", "Art", "June 10, 2020", "2.0.0", "3.0.0 and up"))
      .toDF("App", "Categories", "Rating", "Reviews", "Size",
        "Installs", "Type", "Price", "Content_Rating", "Genres",
        "Last_Updated", "Current_Version", "Minimum_Android_Version")

    val expected = Seq(
      ("Games", 2, (4.0 + 4.5) / 2, 0.5 / 2),
      ("Art", 1, 3.0, 0.0))
      .toDF("Genre", "Count", "Average_Rating", "Average_Sentiment_Polarity")

    val df_5 = App.part5(inputDf1, inputDf2)

    assertResult(expected.collect())(df_5.collect())
  }
}