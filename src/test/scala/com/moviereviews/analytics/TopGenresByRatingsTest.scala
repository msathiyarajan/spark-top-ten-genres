package com.moviereviews.analytics.test

import org.scalatest._
import com.holdenkarau.spark.testing._
import org.apache.spark.sql.types.{
  StringType,
  StructField,
  StructType,
  DoubleType,
  LongType,
  IntegerType
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ udf, explode, lit, round }
import com.moviereviews.analytics.{ TopGenresByRatings, TopGenresByRatingsHelper }

/**
 * @author Nilanjan Sarkar
 * @since  2017/08/17
 * @version 1.0.0
 *
 * Tester application for : TopGenresByRatings
 */
class TopGenresByRatingsTest extends FunSuite with DataFrameSuiteBase {

  test("Test Tokenizer UDF") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val schema = StructType(Seq(StructField("genre", StringType, nullable = true)))

    val testData = Seq(Row("A,B,C"), Row("D,E,F"))
    val rddTest = sc.parallelize(testData)
    val testDF = sqlCtx.createDataFrame(rddTest, schema)

    val expectedData =
      Seq(Row("A"), Row("B"), Row("C"), Row("D"), Row("E"), Row("F"))
    val rddExpected = sc.parallelize(expectedData)
    val expectedDF = sqlCtx.createDataFrame(rddExpected, schema)

    val actualDf = testDF.withColumn(
      "genre",
      explode(TopGenresByRatingsHelper.tokenizerUDF($"genre", lit(","))))

    assertDataFrameEquals(expectedDF, actualDf)
  }

  test("Test Weighted Average UDF") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDataschema = StructType(
      Seq(
        StructField("mean_user_rating", StringType, nullable = true),
        StructField("votes_by_genre", StringType, nullable = true)))

    val finalSchema = StructType(
      Seq(
        StructField("mean_user_rating", StringType, nullable = true),
        StructField("votes_by_genre", StringType, nullable = true),
        StructField("wt_rating", DoubleType, nullable = true)))

    val testData =
      Seq(Row("6.66", "145000"), Row("7.81", "81060"), Row("5.58", "79060"))
    val rddTest = sc.parallelize(testData)
    val testDF = sqlCtx.createDataFrame(rddTest, inputDataschema)

    val expectedData = Seq(
      Row("6.66", "145000", 6.66),
      Row("7.81", "81060", 7.8),
      Row("5.58", "79060", 5.6))
    val rddExpected = sc.parallelize(expectedData)
    val expectedDF = sqlCtx.createDataFrame(rddExpected, finalSchema)

    val actualDf = testDF.withColumn(
      "wt_rating",
      round(
        TopGenresByRatingsHelper.weightedRatingUDF(
          $"mean_user_rating",
          $"votes_by_genre",
          lit(1000),
          lit(7.0)),
        2))

    assertDataFrameEquals(expectedDF, actualDf)
  }

  test("Top 10 genre by ratings test") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val schema = StructType(
      Array(
        StructField("genre", StringType, nullable = true),
        StructField("rank", IntegerType, nullable = true)))

    // prepare test data
    val data = Seq(
      Row("Thriller", 1),
      Row("Police", 2),
      Row("Military", 3),
      Row("Psychological", 4),
      Row("Dementia", 5),
      Row("Josei", 6),
      Row("Historical", 7),
      Row("Drama", 8),
      Row("Mystery", 9),
      Row("Samurai", 10))

    val rdd = sc.parallelize(data)
    val testData = sqlCtx.createDataFrame(rdd, schema)

    // run the actual code with data provided
    val animePath = "src/test/resources/data/anime.csv"
    val ratingPath = "src/test/resources/data/rating.csv.gz"
    val typ = "TV"
    val noOfEpisodes = "10"
    val ranks = "10"

    val algoClass = new TopGenresByRatings
    val rankData = algoClass.getTopNGenreByRatings(
      animePath,
      ratingPath,
      typ,
      noOfEpisodes,
      ranks,
      sc)

    assertDataFrameEquals(testData, rankData)

    // Show the output
    rankData.show(false)
  }
}
