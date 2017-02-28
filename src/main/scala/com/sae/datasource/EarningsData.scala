package com.sae.datasource

import java.time.{LocalDate}

/**
  * case class for earnings data
  * releaseDate - the date that earnings were released
  * qtr - what quarter the earnings are for
  * User: Cliff
  * Date: 2/21/2017
  * Time: 2:44 PM
  */
case class EarningsData( quote:String, releaseDate:LocalDate, qtr:Int )

object EarningsData {

  //ordering for Java Local Dates
  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by( _.toEpochDay )

  implicit def orderingByQtrRelDate[A <: EarningsData]: Ordering[EarningsData] =
    Ordering.by( ed => ( ed.qtr, ed.releaseDate ) )
}
