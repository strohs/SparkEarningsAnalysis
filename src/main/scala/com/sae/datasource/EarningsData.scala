package com.sae.datasource

import java.sql.Date
import java.time.LocalDate

/**
  * case class for earnings data
  * releaseDate - the date that earnings were released
  * qtr - what quarter the earnings are for
  * User: Cliff
  * Date: 2/21/2017
  * Time: 2:44 PM
  */
case class EarningsData( quote:String, releaseDate:Date, qtr:Int )

object EarningsData {

  //ordering for Java Local Dates
  implicit val localDateOrdering: Ordering[Date] = Ordering.by( _.toLocalDate.toEpochDay )

  implicit def orderingByQtrRelDate[A <: EarningsData]: Ordering[EarningsData] =
    Ordering.by( ed => ( ed.qtr, ed.releaseDate ) )
}
