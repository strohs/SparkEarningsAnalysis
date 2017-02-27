package com.sae

import java.time.{Duration, LocalDate, Period}

import com.sae.analyzer.MoveAnalyzer
import com.sae.datasource._
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.immutable.HashMap


/**
  * RDD version of the Earnings Trend analysis tool.
  * User: Cliff
  * Date: 2/26/2017
  * Time: 1:58 PM
  */
object EarningsTrendRDD {

  //might possibly need this to partition RDDs by date
  //implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)
  // OR
  //implicitly[Ordering[java.time.chrono.ChronoLocalDate]]

  val SPARK_MASTER          = "spark://Endymion:7077"

  val LOOK_BACK_YEARS = 4
  val LOOK_BEFORE_DAYS = 21
  val LOOK_AFTER_DAYS = 21
  val MIN_SLIDE_RANGE = 3

  def maxMoveBetweenDates( priceData:List[PriceData], beginDate:LocalDate, endDate:LocalDate ): PriceMove = {
    val prices = priceData.filter( pd => DateUtils.dateBetween( pd.date, beginDate, endDate ) )
    MoveAnalyzer.priceMoveByRanges( prices, MIN_SLIDE_RANGE )
      .flatten.maxBy( pm => Math.abs( pm.amount) )
  }

  def maxMovesAroundDate( earningsData: EarningsData, priceData: List[PriceData] ): (PriceMove, PriceMove) = {
    val maxBefore = maxMoveBetweenDates( priceData, earningsData.releaseDate.minusDays( LOOK_BEFORE_DAYS), earningsData.releaseDate )
    val maxAfter = maxMoveBetweenDates( priceData, earningsData.releaseDate, earningsData.releaseDate.plusDays( LOOK_AFTER_DAYS ))
    (maxBefore, maxAfter)
  }

  def movesByQuarter( quote:String, qtr:Int, priceData:List[PriceData], earningsData:List[EarningsData] ): List[EarningsReleaseMove] = {
    val startDate = LocalDate.now().minusYears( LOOK_BACK_YEARS )
    val endDate = LocalDate.now()

    //get all earnings releases by quarter for the last four years
    val quarterlyEarnings = earningsData.filter( ed => ed.qtr == qtr &&
      ed.releaseDate.getYear >= LocalDate.now.getYear - LOOK_BACK_YEARS )

    //for each release date in quarterlyEarnings, find the max price move that occured within X number of days before
    // the release and X num days after the release
    val earningsMove = quarterlyEarnings.map { qd =>
      val (before,after) = maxMovesAroundDate( qd, priceData )
      EarningsReleaseMove( qd, before, after )
    }
    earningsMove
  }

  def main( args: Array[ String ] ): Unit = {
    val conf = new SparkConf().setAppName( "Earnings Release Trends" ).setMaster( SPARK_MASTER )
    val sc = new SparkContext( conf )

    //consider passing these on the command line
    val quotes = List("ICE")
    val startDate = LocalDate.now().minusYears( LOOK_BACK_YEARS )
    val NOW = LocalDate.now()
    val startOfYear = startDate.withMonth(1).withDayOfMonth(1)
    val endOfLastYear = NOW.minusYears(1).withMonth(12).withDayOfMonth(31)


    //get stock prices for each quote in the list,  going back for a max of LOOK_BACK_YEARS
    val priceData:List[PriceData] = quotes.flatMap( q => Yahoo.getDailyPrices( q, startDate, NOW ) )
    println( s"retrieved ${priceData.length} prices" )
    //create the prices RDD
    val pricesRDD = sc.parallelize( priceData )

    //get the last LOOK_BACK_YEARS worth of earnings releases, but don't include any info from the current year
    val earningsData:List[EarningsData] = quotes.flatMap( q => Zacks.getEarningsDates( q ) )
    println( s"retrieved ${earningsData.length} earnings dates" )
    val earningsRDD = sc.parallelize( earningsData )

    //arrange earnings data into a PairRDD of earnings dates by quarter: ((quote,qtr), Iterable[LocalDate]) and
    //be sure to filter out the current years earnings data
    val earningsForQtr = earningsRDD.filter( ed =>
      DateUtils.dateBetween( ed.releaseDate, startOfYear, endOfLastYear) ).map( ed => {
      (( ed.quote,ed.qtr ), ed.releaseDate )
    }).groupByKey()

    earningsForQtr.foreach( println )

//    val earningsMap = quotes.foldLeft( new HashMap[String,List[EarningsData]]() )( (edMap, quote) =>
//      edMap + ( quote -> earningsData.filter( _.quote.equals(quote)) )
//    )

    //store earnings releases in a broadcast variable. Earnings data should be much smaller than price data
    //val earningsBV = sc.broadcast( earningsMap )


    
    //have spark group prices by quote symbol and then partition by quote
    //val pricesByQuote = pricesRDD.groupBy( pd => pd.quote ).partitionBy( new spark.HashPartitioner( quotes.length ) )


//    val trendByQuarter = quotes.map( quote => {
//      movesByQuarter( quote, 1, pricesRDD, earningsBV.value )
//    })





  }

}
