package com.sae.datasource

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.io.Source


/**
  * functions to retrieve stock price data from Yahoo finance
  * User: Cliff
  * Date: 2/21/2017
  * Time: 2:20 PM
  */
object Yahoo {

  //the pattern we are expecting our start/end dates to be in  mm-dd-yyyy
  val DATE_PATTERN = """(\d{1,2})[/-](\d{1,2})[/-](\d\d\d\d)""".r


  /**
    * parse a date into a mm,dd,yyyy tuple
    * @param date a date String in mm-dd-yyyy format
    * @return (mm,dd,yyyy) 3-tuple of String
    */
  def parseDate( date:String ): (String,String,String) = date match {
    case DATE_PATTERN( month, day, year) => (month,day,year)
    case _ => throw new IllegalArgumentException(s"$date must be in mm-dd-yyyy format")
  }


  private def buildPriceURL( quote:String, startDate:LocalDate, endDate:LocalDate ):String = {
    //months are 0 based at Yahoo quote service, we have to subtract 1 from the months below
    var (sm, sd, sy) = ( startDate.getMonthValue, startDate.getDayOfMonth, startDate.getYear )
    sm = sm - 1
    var (em, ed, ey) = ( endDate.getMonthValue, endDate.getDayOfMonth, endDate.getYear )
    em = em - 1
    s"http://chart.finance.yahoo.com/table.csv?s=$quote&a=$sm&b=$sd&c=$sy&d=$em&e=$ed&f=$ey&g=d&ignore=.csv"
  }

  /**
    * yahoo finance returns the following price data seperated by comma:
    * Date,Open,High,Low,Close,Volume,Adj Close
    *
    * @param quote
    * @param startDate
    * @param endDate
    * @return
    */
  def getDailyPrices( quote:String, startDate:LocalDate, endDate:LocalDate ):List[PriceData] = {

    val url = buildPriceURL( quote, startDate, endDate )
    //gather the data from Yahoo into a list, then split the CSV into another list, and store the values we want to
    // work with into the PriceData case class
    val source = Source.fromURL( url )
    val lines = try source.getLines.drop(1).foldLeft( List[PriceData]() ) { ( lines, line ) =>
      val data = line.split(",")
      PriceData( quote, LocalDate.parse( data(0) ), data( 6 ).toDouble ) +: lines
    } finally source.close()
    lines
  }

}
