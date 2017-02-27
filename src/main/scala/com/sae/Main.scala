package com.sae

import java.time._

import com.sae.analyzer.MoveAnalyzer
import com.sae.datasource._



/**
  * This is the pure Scala version of the Earnings Analysis tool. It doesn't use Spark, and it used to make sure the
  * analysis code is working
  * User: Cliff
  * Date: 2/20/2017
  * Time: 6:02 PM
  */
object Main {

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
    val startDate = LocalDate.now().minusYears( 5 )
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
    val quote = "ICE"
    val startDate = LocalDate.now().minusYears( 5 )
    val endDate = LocalDate.now()


    //get all prices for the last X=4 number of years, these prices will be in ascending order
    val priceData = Yahoo.getDailyPrices( quote, startDate, endDate )
    println( s"retrieved ${priceData.length} prices" )
    //get all earnings release dates (will have last ten years) and throw out all dates from the current year
    val earningsData = Zacks.getEarningsDates( quote ).filterNot( ed => ed.releaseDate.getYear == LocalDate.now().getYear )
    println( s"retrieved ${earningsData.length} earnings dates" )


//    val qms = movesByQuarter( quote,1, priceData, earningsData )
//    qms.foreach { erm =>
//        println( s"${erm.earningsData.releaseDate}  ${erm.maxMovePrior}  ||  ${erm.maxMoveAfter}" )
//      }



  }

}
