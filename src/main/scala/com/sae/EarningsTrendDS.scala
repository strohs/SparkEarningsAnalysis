package com.sae

import java.sql.Date
import java.time.LocalDate

import com.sae.analyzer.MoveAnalyzer
import com.sae.datasource.{PriceData, Yahoo, Zacks}
import org.apache.spark.sql.{Encoders, SparkSession}


/**
  * Dataset/Dataframe version of EarningsTrendRDD
  */
object EarningsTrendDS {



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

    val pricesRaw = Yahoo.getDailyPrices( quote, startDate, NOW )
    println(s"read ${pricesRaw.length} prices from Yahoo")

    val earningsRaw = Zacks.getEarningsDates( quote )
    println(s"read ${earningsRaw.length} earnings release dates")

    val prices = pricesRaw.toDS()
    println( prices.show() )
    val earningsDates = earningsRaw.toDS()
    println( earningsDates.show() )

    //get earnings from the last 3 years and don't include the current year
    val earningsDS = earningsDates.filter(s"releaseDate > '${Date.valueOf(fourYearsAgo)}' AND releaseDate < '${Date.valueOf(endOfLastYear)}'")
      .sort("qtr","releaseDate")

    val es = earningsDS.groupByKey( ed => (ed.qtr, ed.releaseDate ) )

    val arr = es.mapGroups( (k,vs) => (k._1, k._2, vs.toList) ).collect()
    //arr.foreach{ case ((q,rd, rds)) => println(s"qtr:$q  rd:$rd  dates:${rds.length}") }
  }

}
