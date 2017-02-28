package com.sae

import java.time.LocalDate

import com.sae.analyzer.MoveAnalyzer
import com.sae.datasource._
import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}


/**
  * RDD version of the Earnings Trend analysis tool.
  * User: Cliff
  * Date: 2/26/2017
  * Time: 1:58 PM
  */
object EarningsTrendRDD {

  //need this to Range partition RDDs by LocalDate
  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by( _.toEpochDay )


  val SPARK_MASTER = "spark://Endymion:7077"

  val LOOK_BACK_YEARS = 3
  val LOOK_BEFORE_WEEKS = 4
  val LOOK_AFTER_WEEKS = 4
  val MIN_SLIDE_RANGE = 3

  def maxMoveBetweenDates( priceData: List[PriceData], beginDate: LocalDate, endDate: LocalDate ): PriceMove = {
    val prices = priceData.filter( pd => DateUtils.dateBetween( pd.date, beginDate, endDate ) )
    if ( !prices.isEmpty )
      MoveAnalyzer.priceMoveByRanges( prices, MIN_SLIDE_RANGE )
        .flatten.maxBy( pm => Math.abs( pm.amount ) )
    else
      PriceMove( beginDate, endDate, 0.0 )
  }

  def maxMovesAroundDate( earningsDate: LocalDate, priceData: List[PriceData] ): (PriceMove, PriceMove) = {
    val maxBefore = maxMoveBetweenDates( priceData, earningsDate.minusWeeks( LOOK_BEFORE_WEEKS ), earningsDate )
    val maxAfter = maxMoveBetweenDates( priceData, earningsDate, earningsDate.plusWeeks( LOOK_AFTER_WEEKS ) )
    (maxBefore, maxAfter)
  }

  //helper function to see how much data is in each partition
  def countByPartition( rdd: RDD[(LocalDate, PriceData)] ) = {
    rdd.mapPartitions( iter => Iterator( iter.length ) )
  }

  def main( args: Array[String] ): Unit = {
    val conf = new SparkConf().setAppName( "Earnings Release Trends" ).setMaster( SPARK_MASTER )
    val sc = new SparkContext( conf )

    //consider passing these on the command line
    val quote = args(0).toUpperCase()
    val startDate = LocalDate.now().minusYears( LOOK_BACK_YEARS )
    val NOW = LocalDate.now()
    val fourYearsAgo = startDate.withMonth( 1 ).withDayOfMonth( 1 )
    val endOfLastYear = NOW.minusYears( 1 ).withMonth( 12 ).withDayOfMonth( 31 )


    //get stock prices for each quote in the list,  going back for a maximum of LOOK_BACK_YEARS + 1
    val priceData: List[PriceData] = Yahoo.getDailyPrices( quote, startDate.minusYears(1), NOW )
    println( s"retrieved ${priceData.length} prices" )
    //create the prices RDD
    val pricesRDD = sc.parallelize( priceData )
    val pricesByDate = pricesRDD.keyBy( pd => pd.date )
    val ppRDD = pricesByDate.partitionBy( new RangePartitioner[LocalDate, PriceData]( 4, pricesByDate ) )


    //println( s"ppRDD count: ${ppRDD.count()}")
    //println( s"partitioner: ${ppRDD.partitioner}")
    //println( s"partitioner length: ${ppRDD.partitions.length}")
    //countByPartition(ppRDD).collect().foreach( println )

    //get the last LOOK_BACK_YEARS worth of earnings releases, but don't include any info from the current year
    val earningsData: List[EarningsData] = Zacks.getEarningsDates( quote )
    println( s"retrieved ${earningsData.length} earnings dates" )
    val earningsRDD = sc.parallelize( earningsData )

    //arrange earnings data into a PairRDD of earnings dates by quarter: ((quote,qtr), Iterable[LocalDate]) and
    //be sure to filter out the current years earnings data
    val earningsByQtr = earningsRDD.filter( ed =>
      DateUtils.dateBetween( ed.releaseDate, fourYearsAgo, endOfLastYear ) ).map( ed => {
      ((ed.quote, ed.qtr), ed.releaseDate)
    } ).groupByKey()

    //store earningsByQtr, as a Map, in a broadcast var
    val earningsMapBV = sc.broadcast( earningsByQtr.collectAsMap() )
    earningsMapBV.value.foreach( println )


    val earningsMoves:Iterable[EarningsReleaseMove] =
      for ( (quote,qtr) <- earningsMapBV.value.keys;
            releaseDate <- earningsMapBV.value( (quote, qtr) ) ) yield {
        val beginDate = releaseDate.minusWeeks( LOOK_BEFORE_WEEKS )
        val endDate = releaseDate.plusWeeks( LOOK_AFTER_WEEKS )
        val priceData = ppRDD.filterByRange( beginDate, endDate ).filter( pd => pd._2.quote == quote ).values.collect().toList
        println( s"--------- releaseDate ${releaseDate} priceData: ${priceData.length}" )
        val moves = maxMovesAroundDate( releaseDate, priceData )
        EarningsReleaseMove( EarningsData( quote, releaseDate, qtr ), moves._1, moves._2 )
      }

    earningsMoves.toList.sorted.foreach( println )

  }

}
