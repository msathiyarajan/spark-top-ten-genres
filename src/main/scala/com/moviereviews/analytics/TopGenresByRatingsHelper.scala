package com.moviereviews.analytics

import org.apache.spark.sql.functions.udf

/**
 * @author Nilanjan Sarkar
 * @since  2017/08/16
 * @version 1.0.0
 *
 * Helper to find top N genres of TV shows by ratings
 */
object TopGenresByRatingsHelper {

  /**
   * Forms the SQL to join Anime data to user ratings
   *
   * @param typ The type of content like TV, Movies
   * @param numOfEp The number   episodes if applicable
   * @return SQL for Join
   */
  def animeToUserRatingSQL(typ: String, numOfEp: String): String = {
    val finalQueryBuiler = StringBuilder.newBuilder
    val selectBuilder = StringBuilder.newBuilder
    val fromBuilder = StringBuilder.newBuilder
    val whereBuilder = StringBuilder.newBuilder

    selectBuilder.append("select ")
    selectBuilder.append("a.type, ")
    selectBuilder.append("a.genre,")
    selectBuilder.append("a.episodes, ")
    selectBuilder.append("a.rating as average_rating,")
    selectBuilder.append("a.members, ")
    selectBuilder.append("u.user_id, ")
    selectBuilder.append("u.rating as user_rating ")

    fromBuilder.append("from ")
    fromBuilder.append(
      "anime_data a inner join user_ratings u on a.anime_id = u.anime_id ")

    fromBuilder.append("where ")
    fromBuilder.append(s"a.type = '${typ}' ")
    fromBuilder.append(s"and a.episodes > ${numOfEp} ")

    finalQueryBuiler
      .append(selectBuilder.toString())
      .append(fromBuilder.toString())
      .append(whereBuilder.toString())
    finalQueryBuiler.toString
  }

  /**
   * Given an input string, this function generates
   * tokens based on a token
   *
   * Ex - "A,B,C,D" is split by "," into an Array as Array("A","B","C","D")
   *
   * @param input The input String
   * @param token The token on which to split
   * @return An array of tokens
   */
  def tokenizer = (input: String, token: String) => {
    import java.util.StringTokenizer
    import scala.collection.mutable.ArrayBuffer
    val res = new ArrayBuffer[String]
    val st = new StringTokenizer(input, token)
    while (st.hasMoreTokens()) {
      res += st.nextToken()
    }
    res
  }

  // Make a UDF for custom tokenizer
  val tokenizerUDF = udf(tokenizer)

  /**
   * The formula for calculating the Top Rated 250 Titles gives a true Bayesian estimate:
   *
   * weighted rating (WR) = (v ÷ (v+m)) × R + (m ÷ (v+m)) × C
   *
   * Where:
   *
   * R = average for the movie (mean) = (Rating)
   * v = number of votes for the movie = (votes)
   * m = minimum votes required to be listed
   * C = the mean vote across the whole report
   *
   * @param rating The average rating for the content
   * @param viewership The number of votes for the content
   * @param minUsers The minimum votes required to be listed
   * @param meanRating The mean vote across the whole data
   * @return The weighted average rating for TV, movies etc
   */
  def weightedRating =
    (rating: String,
      viewership: String,
      minUsers: Long,
      meanRating: Double) => {

      val r: Double = rating.toDouble
      val v: Float = viewership.toFloat
      val m: Float = minUsers.toFloat
      val c = meanRating
      val wr = (v / (v + m)) * r + (m / (v + m)) * c
      wr
    }

  // Make a UDF for custom weight function
  val weightedRatingUDF = udf(weightedRating)
}
