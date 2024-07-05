package org.opensource.sparkchallenge

import org.apache.spark.sql.functions.{avg, col, desc}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Bruno Silva
 */

object App {

  def main(args : Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("spark-challenge")
      .getOrCreate()

    val googlePlayStoreDf = spark.read
      .option("header", "true")
      // default value is STOP_AT_DELIMITER which causes commas inside quotes to be read as a delimiter
      .option("unescapedQuoteHandling", "STOP_AT_CLOSING_QUOTE")
      .csv("src/main/resources/googleplaystore.csv")

    val googlePlayStoreUserReviewsDf = spark.read
      .option("header", "true")
      // default value is STOP_AT_DELIMITER which causes commas inside quotes to be read as a delimiter
      .option("unescapedQuoteHandling", "STOP_AT_CLOSING_QUOTE")
      .csv("src/main/resources/googleplaystore_user_reviews.csv")

    val df_1 = part1(googlePlayStoreUserReviewsDf)

    val df_2 = part2(googlePlayStoreDf)

    spark.stop()
  }

  private def part1(inputDf: DataFrame): DataFrame = {

    val df = inputDf
      .na.replace("Sentiment_Polarity", Map("nan" -> "0"))
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("Double"))
      .groupBy("App")
      .avg("Sentiment_Polarity")
      .withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")

    df
  }

  private def part2(inputDf: DataFrame): DataFrame = {

    // NOTE: The Life Made Wi-Fi Touchscreen Photo Frame app does not have a category,
    //       so values shift left by one, leaving it with the best rating because it has 19 reviews.
    val df = inputDf
      .na.replace("Rating", Map("NaN" -> "0"))
      .withColumn("Rating", col("Rating").cast("Double"))
      .filter("Rating >= 4")
      .orderBy(desc("Rating"))

    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("delimiter", "§")
      .csv("output/best_aps")

    df
  }

}
