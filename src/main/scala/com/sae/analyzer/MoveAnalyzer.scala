package com.sae.analyzer

import com.sae.datasource.{PriceData, PriceMove}

/**
  * functions to compute how much at price moves within a date range or within multiple date ranges. These functions
  * will primarily return the movement information within a PriceMove case class
  * User: Cliff
  * Date: 2/24/2017
  * Time: 1:34 PM
  */
object MoveAnalyzer {

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

  
}
