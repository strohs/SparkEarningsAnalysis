package com.sae.datasource

import java.time.LocalDate

/**
  * holds data pertaining to how a price moves before and after a company's earnings release date
  * @param earningsData - case class that holds the earnings release date and the quarter the earnings are for
  * @param maxMovePrior - the maximum amount a price moved prior to earnings release date
  * @param maxMoveAfter - the maximum amount a price moved after an earnings release date
  */
case class EarningsReleaseMove( earningsData:EarningsData, maxMovePrior:PriceMove, maxMoveAfter:PriceMove )
