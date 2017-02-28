package com.sae

import java.time.{Duration, LocalDate, Period}

import com.sae.analyzer.MoveAnalyzer
import com.sae.datasource._
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

import scala.collection.immutable.HashMap


/**
  * RDD version of the Earnings Trend analysis tool.
  * User: Cliff
  * Date: 2/26/2017
  * Time: 1:58 PM
  */
object EarningsTrendRDD {

  //need this to Range partition RDDs by LocalDate
  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)
  // OR
  //implicitly[Ordering[java.time.chrono.ChronoLocalDate]]

  val SPARK_MASTER          = "spark://Endymion:7077"

  val LOOK_BACK_YEARS = 4
  val LOOK_BEFORE_WEEKS = 4
  val LOOK_AFTER_WEEKS = 4
  val MIN_SLIDE_RANGE = 3

  def maxMoveBetweenDates( priceData:List[PriceData], beginDate:LocalDate, endDate:LocalDate ): PriceMove = {
    val prices = priceData.filter( pd => DateUtils.dateBetween( pd.date, beginDate, endDate ) )
    MoveAnalyzer.priceMoveByRanges( prices, MIN_SLIDE_RANGE )
      .flatten.maxBy( pm => Math.abs( pm.amount) )
  }

  def maxMovesAroundDate( earningsDate:LocalDate, priceData: List[PriceData] ): (PriceMove, PriceMove) = {
    val maxBefore = maxMoveBetweenDates( priceData, earningsDate.minusWeeks( LOOK_BEFORE_WEEKS ), earningsDate )
    val maxAfter = maxMoveBetweenDates( priceData, earningsDate, earningsDate.plusWeeks( LOOK_AFTER_WEEKS ))
    (maxBefore, maxAfter)
  }

//  def movesByQuarter( quote:String, qtr:Int, priceData:List[PriceData], earningsData:List[EarningsData] ): List[EarningsReleaseMove] = {
//    val startDate = LocalDate.now().minusYears( LOOK_BACK_YEARS )
//    val endDate = LocalDate.now()
//
//    //get all earnings releases by quarter for the last four years
//    val quarterlyEarnings = earningsData.filter( ed => ed.qtr == qtr &&
//      ed.releaseDate.getYear >= LocalDate.now.getYear - LOOK_BACK_YEARS )
//
//    //for each release date in quarterlyEarnings, find the max price move that occured within X number of days before
//    // the release and X num days after the release
//    val earningsMove = quarterlyEarnings.map { qd =>
//      val (before,after) = maxMovesAroundDate( qd, priceData )
//      EarningsReleaseMove( qd, before, after )
//    }
//    earningsMove
//  }

  //helper function to see how much data is in each partition
  def countByPartition(rdd: RDD[(LocalDate, PriceData)]) = {
    rdd.mapPartitions(iter => Iterator(iter.length))
  }

  def main( args: Array[ String ] ): Unit = {
    val conf = new SparkConf().setAppName( "Earnings Release Trends" ).setMaster( SPARK_MASTER )
    val sc = new SparkContext( conf )

    //consider passing these on the command line
    val quotes = List("ICE","UA")
    val startDate = LocalDate.now().minusYears( LOOK_BACK_YEARS )
    val NOW = LocalDate.now()
    val startOfYear = startDate.withMonth(1).withDayOfMonth(1)
    val endOfLastYear = NOW.minusYears(1).withMonth(12).withDayOfMonth(31)


    //get stock prices for each quote in the list,  going back for a max of LOOK_BACK_YEARS
    val priceData:List[PriceData] = quotes.flatMap( q => Yahoo.getDailyPrices( q, startDate, NOW ) )
    println( s"retrieved ${priceData.length} prices" )
    //create the prices RDD
    val pricesRDD = sc.parallelize( priceData )
    val pricesByDate = pricesRDD.keyBy( pd => pd.date )
    val ppRDD = pricesByDate.partitionBy( new RangePartitioner[LocalDate,PriceData](4,pricesByDate) )
    ppRDD.persist()

    //println( s"ppRDD count: ${ppRDD.count()}")
    //println( s"partitioner: ${ppRDD.partitioner}")
    //println( s"partitioner length: ${ppRDD.partitions.length}")
    //countByPartition(ppRDD).collect().foreach( println )

    //get the last LOOK_BACK_YEARS worth of earnings releases, but don't include any info from the current year
    val earningsData:List[EarningsData] = quotes.flatMap( q => Zacks.getEarningsDates( q ) )
    println( s"retrieved ${earningsData.length} earnings dates" )
    val earningsRDD = sc.parallelize( earningsData )

    //arrange earnings data into a PairRDD of earnings dates by quarter: ((quote,qtr), Iterable[LocalDate]) and
    //be sure to filter out the current years earnings data
    val earningsByQtr = earningsRDD.filter( ed =>
      DateUtils.dateBetween( ed.releaseDate, startOfYear, endOfLastYear) ).map( ed => {
      (( ed.quote,ed.qtr ), ed.releaseDate )
    }).groupByKey()

    //store earningsByQtr, as a Map, in a broadcast var
    val earningsMapBV = sc.broadcast( earningsByQtr.collectAsMap() )
    //earningsMapBV.value.foreach( println )

    val earningsDate:LocalDate = earningsMapBV.value( ("ICE",1) ).head
    println( s"FIRST DATE: $earningsDate" )
    val beginDate = earningsDate.minusWeeks( LOOK_BEFORE_WEEKS )
    val tdates = ppRDD.filterByRange( beginDate, earningsDate.plusWeeks( LOOK_AFTER_WEEKS ) )
      .filter( pd => pd._2.quote == "ICE" )
      .values.collect().toList

    val maxMove = maxMovesAroundDate( earningsDate, tdates )

    //for all quotes
    //  for all qtr  1 to 4
    //    val qtrRelDates = earningsMapBV( (quote,qtr) )
    //    for relDate in qtrRelDates
    //      val beginDate = relDate.minusWeeks( LOOK_BEFORE_WEEKS )
    //      val endDate = relDate.plusWeeks( LOOK_AFTER_WEEKS )
    //      val priceData = ppRDD.filterByRange( beginDate, endDate ).filter( pd => pd._2.quote == quote ).values.collect().toList
    //      maxMove = maxMovesAroundDate( relDate, priceData )
    //      yield ( EarningsReleaseMove( (EarningsData( quote, relDate, qtr), maxMove._1, maxMove._2 )) )





  }

}
