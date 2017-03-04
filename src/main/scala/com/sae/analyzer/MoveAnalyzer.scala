package com.sae.analyzer

import java.sql.Date
import java.time.LocalDate

import com.sae.DateUtils
import com.sae.datasource.{PriceData, PriceMove}

/**
  * functions to compute how much at price moves within a date range or within multiple date ranges. These functions
  * will primarily return the movement information within a PriceMove case class
  * User: Cliff
  * Date: 2/24/2017
  * Time: 1:34 PM
  */
object MoveAnalyzer {

  val LOOK_BACK_YEARS = 3
  val LOOK_BEFORE_WEEKS = 4
  val LOOK_AFTER_WEEKS = 4

  /**
    * determines how much a price moved starting from the beginning of priceData to the end
    *
    * @param priceData list of PriceData sorted by date ascending
    * @return PriceMove case class containing the amount of the price movement
    */
  def getPriceMovement( priceData: List[PriceData] ): PriceMove = {
    PriceMove( priceData.head.date, priceData.last.date, priceData.head.close - priceData.last.close )
  }

  /**
    * examine the List of PriceData to determine how much a price moved within a specified period (range). A sliding
    * window of length, range, is used on the priceData list to compute the price movement within the range. The window is
    * then moved forward by one element and the process is repeated until we reach the end of the priceData List.
    *
    * @param priceData List of PriceData sorted by date ascending
    * @param range     the "width" of the sliding window
    * @return List[PriceMove] containing the details of each move at each range
    */
  def priceMoveByRange( priceData: List[PriceData], range: Int ): List[PriceMove] = {
    priceData.sliding( range, 1 ).foldLeft( List[PriceMove]() ) { ( pms, pds ) =>
      pms :+ getPriceMovement( pds )
    }

  }

  /**
    * This function computes all price moves within priceData by using a sliding window to examine the moves
    * at various sub-ranges. It is just like priceMoveByRange except the range is increased from minRange up to
    * priceData.length at each iteration
    *
    * @param priceData
    * @param minRange
    * @return List[ List[PriceMove] ]
    */
  def priceMoveByRanges( priceData: List[PriceData], minRange: Int = 3 ): List[List[PriceMove]] = {
    minRange.to( priceData.length ).foldLeft( List[List[PriceMove]]() ) { ( pml, range ) =>
      pml :+ priceMoveByRange( priceData, range )
    }
  }

  /**
    * determine the maxmimum move of a quote in the List of PriceData from begin date to end date inclusive.
    * @param priceData list of PriceData to examine, sorted by PriceData.date
    * @param beginDate
    * @param endDate
    * @return a PriceMove case class containing details of the largest move
    */
  def maxMoveBetweenDates( priceData: List[PriceData], beginDate: LocalDate, endDate: LocalDate ): PriceMove = {
    val prices = priceData.filter( pd => DateUtils.dateBetween( pd.date.toLocalDate, beginDate, endDate ) )
    if ( !prices.isEmpty )
      MoveAnalyzer.priceMoveByRanges( prices )
        .flatten.maxBy( pm => Math.abs( pm.amount ) )
    else
      PriceMove( Date.valueOf(beginDate), Date.valueOf(endDate), 0.0 )
  }

  /**
    * determine the maximum move a quote for a certain number of days before and after a target date.
    * @param targetDate a LocalDate that we want to examine the price movement around. It is not included in the
    *                   movement calculations
    * @param priceData List of PriceDate
    * @return A Tuple containing (PriceMove before the targetDate, and after the targetDate)
    */
  def maxMovesAroundDate(targetDate: LocalDate, priceData: List[PriceData] ): (PriceMove, PriceMove) = {
    val maxBefore = maxMoveBetweenDates( priceData, targetDate.minusWeeks( LOOK_BEFORE_WEEKS ), targetDate )
    val maxAfter = maxMoveBetweenDates( priceData, targetDate, targetDate.plusWeeks( LOOK_AFTER_WEEKS ) )
    (maxBefore, maxAfter)
  }

  
}
