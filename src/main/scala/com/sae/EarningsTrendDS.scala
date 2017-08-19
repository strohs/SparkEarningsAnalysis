package com.sae

import java.sql.Date
import java.time.LocalDate
import java.util

import breeze.linalg.max
import com.sae.analyzer.MoveAnalyzer
import com.sae.datasource._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import scala.collection.immutable.HashMap


/**
  * Dataset/Dataframe version of EarningsTrendRDD
  */
object EarningsTrendDS {

  //needed to convert LocalDate(s) to SQL Dates for use in DataSets
  implicit def localDate2SqlDate( d: LocalDate ): java.sql.Date = java.sql.Date.valueOf( d )
  implicit def date2SqlDate( d:Date ): LocalDate = d.toLocalDate


  //for some reason this in needed in Spark 2.1.0 to force encoding of java.sql.Date. According to Spark docs, it should have been handled implicitly
  implicit val encodeDate = org.apache.spark.sql.Encoders.DATE

  def main(args: Array[String]): Unit = {

    //consider passing these on the command line
    val quote = args(0).toUpperCase()
    val startDate = LocalDate.now().minusYears( MoveAnalyzer.LOOK_BACK_YEARS )
    val NOW = LocalDate.now()
    val fourYearsAgo = startDate.withMonth( 1 ).withDayOfMonth( 1 )
    val endOfLastYear = NOW.minusYears( 1 ).withMonth( 12 ).withDayOfMonth( 31 )

    val spark = SparkSession
      .builder()
      .appName("Earnings Trend Analysis DS")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    //get price data for the last (LOOK_BACK_YEARS) worth of data
    val pricesRaw = Yahoo.getDailyPrices( quote, startDate, NOW )
    val pricesDS = spark.createDataset( pricesRaw )
    //println( pricesDS.show() )

    //get earnings releases data from our datsource and only use dates from the previous 4 years
    val earningsDS = Zacks.getEarningsDates( quote ).toDS
      .filter( s"releaseDate BETWEEN '${fourYearsAgo}' AND '${endOfLastYear}'" )

    //convert the dataset into a Map of earnings Releases by quarter:  Map[qtr,List[EarningsData]]
    val earningsMap = earningsDS.map( ed => ed.qtr -> ed )
      .collect()
      .groupBy(_._1).map { case (k,v) => ( k, v.map(_._2) ) }


    val results:Iterable[EarningsReleaseMove] = for ( qtr <- earningsMap.keys;
          earningsData <- earningsMap(qtr) ) yield {
      val begin = earningsData.releaseDate.minusWeeks( MoveAnalyzer.LOOK_BEFORE_WEEKS )
      val end = earningsData.releaseDate.plusWeeks( MoveAnalyzer.LOOK_AFTER_WEEKS )

      val pricesBefore = pricesDS.filter(s"date BETWEEN '$begin' AND '${earningsData.releaseDate.minusDays(1)}'")
        .collect().toList
      val pricesAfter = pricesDS.filter(s"date BETWEEN '${earningsData.releaseDate.plusDays(1)}' AND '$end'")
        .collect().toList
      val maxBefore = MoveAnalyzer.maxMove( pricesBefore )
      val maxAfter = MoveAnalyzer.maxMove( pricesAfter )
      EarningsReleaseMove( earningsData, maxBefore, maxAfter )
    }
    results.toList.sorted.foreach( println )

    // qtr, relDate, bMoveStart, bMoveEnd, bMove, aMoveStart, aMoveEnd, aMove
    //MAY have to convert negative move values to positive
    val resultsDF = results.map { erm =>
      ResultDF(erm.earningsData.qtr, erm.earningsData.releaseDate, erm.maxMovePrior.start,
        erm.maxMovePrior.end, Math.abs(erm.maxMovePrior.amount), erm.maxMoveAfter.start, erm.maxMoveAfter.end,
        Math.abs(erm.maxMoveAfter.amount) )
    }.toList.toDF().sort($"qtr",$"relDate")

    println( resultsDF.show() )
    //save generated data as a .csv in users home directory
    resultsDF.coalesce(1).write.option("header","true").csv(s"~/$quote.csv")
  }

}
