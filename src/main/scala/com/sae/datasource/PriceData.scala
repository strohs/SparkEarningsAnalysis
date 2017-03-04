package com.sae.datasource

import java.sql.Date
import java.time.LocalDate


/**
  * Created with IntelliJ IDEA.
  * User: Cliff
  * Date: 2/21/2017
  * Time: 2:55 PM
  */
case class PriceData( quote:String, date:Date, close:Double, adjClose:Double )



object PriceData {

  //needed to convert LocalDate(s) to SQL Dates for use in DataSets
  //implicit def localDate2SqlDate( d: LocalDate ): java.sql.Date = java.sql.Date.valueOf( d )
  //implicit def date2SqlDate( d:Date ): LocalDate = d.toLocalDate

}
