package com.moviereviews.analytics

import com.moviereviews.analytics.TopGenresByRatingsHelper._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
 * @author Nilanjan Sarkar
 * @since  2017/08/16
 * @version 1.0.0
 *
 * Spark application to find top 10 genres of TV shows by ratings
 */
class TopGenresByRatings {

  /**
   * Finds top N genres of TV shows by ratings
   *
   * @param animePath Location of anime data file
   * @param ratingPath Location of ratings data file
   * @param typ Type of content ( TV, Movies etc )
   * @param noOfEpisodes Number of episodes to filter by
   * @param uptoRanks Ranks to see ( Ex - top 10 )
   * @return A DataFrame containing result
   */
  def getTopNGenreByRatings(
    animePath: String,
    ratingPath: String,
    typ: String,
    noOfEpisodes: String,
    uptoRanks: String,
    sc: SparkContext): DataFrame = {

    val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._

    val animeData = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(animePath)

    // Tokenize the multi-valued genre values by comma
    val tokenizedGenre = animeData.withColumn("genre", tokenizerUDF($"genre", lit(",")))

    // De-normalize the genres
    val deNormAnimeData = tokenizedGenre
      .withColumn("genre", explode($"genre"))
      .withColumn("genre", trim($"genre"))
    deNormAnimeData.registerTempTable("anime_data")

    val ratingData = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(ratingPath)
    ratingData.registerTempTable("user_ratings")

    // Find the mean rating across all users
    val meanRating = ratingData
      .selectExpr("round(mean(rating),2) as mean_rating")
      .collect()(0)
      .getAs[Double]("mean_rating")

    // Join Anime data with user ratings
    val joinedData = sqlContext.sql(animeToUserRatingSQL(typ, noOfEpisodes))

    /*
     *  Group data by genres and find the mean user rating across each group.
     *
     *  Then count the votes across each genre group
     */
    val avgUsrRatings = joinedData
      .groupBy("genre")
      .agg(
        mean("user_rating").as("mean_user_rating"),
        count("user_id").as("votes_by_genre"))
      .withColumn("mean_user_rating", round($"mean_user_rating", 2))

    // Find the Bayesian weighted average rating using IMDB top 250 formula
    val wtRatings = avgUsrRatings.withColumn(
      "wt_rating",
      round(
        weightedRatingUDF(
          $"mean_user_rating",
          $"votes_by_genre",
          lit(1000),
          lit(meanRating)),
        2))

    // Order data in descending order by the weighted rating
    val spec = Window.orderBy($"wt_rating".desc)

    // Rank the data up to N ( from input specified )
    wtRatings
      .withColumn("rank", row_number().over(spec))
      .where($"rank" <= uptoRanks.toInt)
      .drop("mean_user_rating")
      .drop("votes_by_genre")
      .drop("wt_rating")
  }
}
