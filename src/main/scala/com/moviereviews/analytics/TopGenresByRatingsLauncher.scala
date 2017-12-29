package com.moviereviews.analytics

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.log4j.{ Level, Logger, LogManager }

/**
 * @author Nilanjan Sarkar
 * @since  2017/08/16
 * @version 1.0.0
 *
 * Spark Launcher Application to find top N genres of TV shows by ratings
 */
object TopGenresByRatingsLauncher extends App {

  if (args.length < 5) {
    throw new IllegalArgumentException(
      "Number of arguments are less. Required arguments are : [ANIME_DATA_PATH] [RATINS_DATA_PATH] [TYPE_OF_CONTENT] [NO_OF_EPISODES] [UPTO_RANK] [OUTPUT_PATH]")
  }

  private val animePath = args(0)
  private val ratingPath = args(1)
  private val typ = args(2)
  private val noOfEpisodes = args(3)
  private val ranks = args(4)
  private val outputPath = args(5)

  private val logger = LogManager.getRootLogger
  logger.setLevel(Level.ERROR)

  private val conf = new SparkConf().setAppName("TopGenresByRatings")
  private val sc = new SparkContext(conf)

  private val algoClass = new TopGenresByRatings
  private val rankData = algoClass.getTopNGenreByRatings(
    animePath,
    ratingPath,
    typ,
    noOfEpisodes,
    ranks,
    sc)

  // Finally write the result
  rankData.write
    .format("com.databricks.spark.csv")
    .mode("overwrite")
    .option("header", "true")
    .save(outputPath)

  logger.info(
    "============================================================================================================================")
  logger.info(" ")
  logger.info(
    s"                         RESULTS STORED SUCCESSFULLY AT : ${outputPath}                                                    ")
  logger.info(
    "============================================================================================================================")
}
