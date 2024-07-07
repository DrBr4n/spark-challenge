package org.opensource.sparkchallenge

import org.apache.spark.sql.functions.{array_distinct, avg, col, collect_list, count, desc, max, max_by, regexp_extract, to_timestamp, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Bruno Silva
 */

object App {

  def main(args : Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("spark-challenge")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    val googlePlayStoreDf = spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv("src/main/resources/googleplaystore.csv")

    val googlePlayStoreUserReviewsDf = spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv("src/main/resources/googleplaystore_user_reviews.csv")

    val df_1 = part1(googlePlayStoreUserReviewsDf)

    val df_2 = part2(googlePlayStoreDf)

    val df_3 = part3(googlePlayStoreDf)

    val df_4 = part4(df_1, df_3)

    val df_5 = part5(df_1, df_3)

    spark.stop()
  }

  def part1(inputDf: DataFrame): DataFrame = {

    val df = inputDf
      .na.replace("Sentiment_Polarity", Map("nan" -> "0"))
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("Double"))
      .groupBy("App")
      .avg("Sentiment_Polarity")
      .withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")

    df
  }

  def part2(inputDf: DataFrame): DataFrame = {

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
      .option("header", "true")
      .option("delimiter", "§")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv("output/best_aps")

    df
  }

  def part3(inputDf: DataFrame): DataFrame = {

    var df = inputDf
      .na.replace(inputDf.columns, Map("NaN" -> "null"))
      .na.replace("Reviews", Map("null" -> "0"))
      .na.replace(Array("App", "Category"), Map("null" -> ""))
      .withColumn("Rating", col("Rating").cast("Double"))
      .withColumn("Reviews", col("Reviews").cast("Long"))
      .withColumn("Size",
        when(col("Size").contains("M"), regexp_extract(col("Size"), "\\d+(\\.\\d+)?", 0).cast("Double"))
          .otherwise(when(col("Size").contains("k"), regexp_extract(col("Size"), "\\d+(\\.\\d+)?", 0).cast("Double") / 1024)
              .otherwise(when(col("Size").contains("G"), regexp_extract(col("Size"), "\\d+(\\.\\d+)?", 0).cast("Double") * 1024))))
      .withColumn("Price",
        when(col("Price").contains("$"), regexp_extract(col("Price"), "\\d+(\\.\\d+)?", 0).cast("Double") * 0.9)
          .otherwise(0.0))
      .withColumn("Last Updated", to_timestamp(col("Last Updated"), "MMMM dd, yyyy"))

    df = df
      .groupBy("App")
      .agg(array_distinct(collect_list("Category")).as("Categories"),
        max_by(col("Rating"), col("Reviews")).as("Rating"),
        max(col("Reviews")).as("Reviews"),
        max_by(col("Size"), col("Reviews")).as("Size"),
        max_by(col("Installs"), col("Reviews")).as("Installs"),
        max_by(col("Type"), col("Reviews")).as("Type"),
        max_by(col("Price"), col("Reviews")).as("Price"),
        max_by(col("Content Rating"), col("Reviews")).as("Content_Rating"),
        array_distinct(collect_list("Genres")).as("Genres"),
        max_by(col("Last Updated"), col("Reviews")).as("Last_Updated"),
        max_by(col("Current Ver"), col("Reviews")).as("Current_Version"),
        max_by(col("Android Ver"), col("Reviews")).as("Minimum_Android_Version"),
      )

    df
  }

  def part4(inputDf1: DataFrame, inputDf2: DataFrame): DataFrame = {

    val df = inputDf2
      .join(inputDf1, Seq("App"))

    df.write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet("output/googleplaystore_cleaned")

    df
  }

  def part5(inputDf1: DataFrame, inputDf2: DataFrame) : DataFrame = {

    val df = inputDf1
      .join(inputDf2, Seq("App"))
      .withColumnRenamed("Genres", "Genre")
      .groupBy("Genre")
      .agg(
        count(col("Genre")).as("Count"),
        avg(col("Rating")).as("Average_Rating"),
        avg(col("Average_Sentiment_Polarity")).as("Average_Sentiment_Polarity")
      )

    df.write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet("output/googleplaystore_metrics")

    df
  }
}
