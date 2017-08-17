package com.sae

import java.sql.Date
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
  implicit val localDateOrdering: Ordering[Date] = Ordering.by( _.toLocalDate.toEpochDay )
  implicit def localDate2SqlDate( d: LocalDate ): java.sql.Date = java.sql.Date.valueOf( d )
  implicit def date2SqlDate( d:Date ): LocalDate = d.toLocalDate

  val SPARK_MASTER = "spark://Straylight:7077"


  //helper function to see how much data is in each partition
  def countByPartition( rdd: RDD[(LocalDate, PriceData)] ) = {
    rdd.mapPartitions( iter => Iterator( iter.length ) )
  }

  def main( args: Array[String] ): Unit = {
    val conf = new SparkConf().setAppName( "Earnings Release Trends" )
      //.setMaster( SPARK_MASTER )
    val sc = new SparkContext( conf )

    //consider passing these on the command line
    val quote = args(0).toUpperCase()
    val startDate = LocalDate.now().minusYears( MoveAnalyzer.LOOK_BACK_YEARS )
    val NOW = LocalDate.now()
    val fourYearsAgo = startDate.withMonth( 1 ).withDayOfMonth( 1 )
    val endOfLastYear = NOW.minusYears( 1 ).withMonth( 12 ).withDayOfMonth( 31 )


    //get stock prices for each quote in the list,  going back for a maximum of LOOK_BACK_YEARS + 1
    val priceData: List[PriceData] = Yahoo.getDailyPrices( quote, startDate.minusYears(1), NOW )
    println( s"retrieved ${priceData.length} prices" )
    //create the prices RDD
    val pricesRDD = sc.parallelize( priceData )
    val pricesByDate = pricesRDD.keyBy( pd => pd.date )
    val ppRDD = pricesByDate.partitionBy( new RangePartitioner[Date, PriceData]( 4, pricesByDate ) )


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
      DateUtils.dateBetween( ed.releaseDate.toLocalDate, fourYearsAgo, endOfLastYear ) ).map( ed => {
      ((ed.quote, ed.qtr), ed.releaseDate)
    } ).groupByKey()

    //store earningsByQtr, as a Map, in a broadcast var
    val earningsMapBV = sc.broadcast( earningsByQtr.collectAsMap() )
    //earningsMapBV.value.foreach( println )


    val earningsMoves:Iterable[EarningsReleaseMove] =
      for ( (quote,qtr) <- earningsMapBV.value.keys;
            releaseDate <- earningsMapBV.value( (quote, qtr) ) ) yield {
        val beginDate = releaseDate.minusWeeks( MoveAnalyzer.LOOK_BEFORE_WEEKS )
        val endDate = releaseDate.plusWeeks( MoveAnalyzer.LOOK_AFTER_WEEKS )
        val priceData = ppRDD.filterByRange( beginDate, endDate ).filter( pd => pd._2.quote == quote ).values.collect().toList
        println( s"--------- releaseDate $releaseDate priceData: ${priceData.length}" )
        val moves = MoveAnalyzer.maxMovesAroundDate( releaseDate, priceData )
        EarningsReleaseMove( EarningsData( quote, releaseDate, qtr ), moves._1, moves._2 )
      }

    earningsMoves.toList.sorted.foreach( println )

  }

}
