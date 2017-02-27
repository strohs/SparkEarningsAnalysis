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
