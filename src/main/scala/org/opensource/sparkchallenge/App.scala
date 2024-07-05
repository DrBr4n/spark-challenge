package org.opensource.sparkchallenge

import org.apache.spark.sql.functions.{avg, col}
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

    val googlePlayStoreUserReviewsDf = spark.read
      .option("header", "true")
      // default value is STOP_AT_DELIMITER which causes commas inside quotes to be read as a delimiter
      .option("unescapedQuoteHandling", "STOP_AT_CLOSING_QUOTE")
      .csv("src/main/resources/googleplaystore_user_reviews.csv")

    val df_1 = part1(googlePlayStoreUserReviewsDf)

    spark.stop()
  }

  private def part1(inputDf: DataFrame): DataFrame = {

    val df = inputDf.select("App", "Sentiment_Polarity")
      .na.replace("Sentiment_Polarity", Map("nan" -> "0"))
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("Double"))
      .groupBy("App")
      .avg("Sentiment_Polarity")
      .withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")

    return df
  }

}
