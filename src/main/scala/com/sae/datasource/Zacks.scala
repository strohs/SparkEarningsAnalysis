package com.sae.datasource

import java.text.SimpleDateFormat
import java.time.{LocalDate, MonthDay, YearMonth}
import java.time.format.DateTimeFormatter
import java.util.Date

import play.api.libs.json.Json

import scala.io.Source

/**
  * functions to retrieve earnings date data from Zacks.com
  *
  * User: Cliff
  * Date: 2/21/2017
  * Time: 2:27 PM
  */
object Zacks {



  /**
    * html scraper that retrieves earnings date data from zacks.com. Zacks stores earnings data for the last 10 years.
    * Zacks embeds the earnings data as a JSON object on the page. This method uses the play json api to parse the dates
    * and quarter info from the json
    * @param quote the stock quote you want earnings data for
    * @return
    */
  def getEarningsDates( quote:String ): List[EarningsData] = {
    val url = s"https://www.zacks.com/stock/research/$quote/earnings-announcements"

    //get raw HTML from our datasource
    val source =  Source.fromURL( url )
    try {
      val rawHTML = source.mkString
      //the regex pattern we use to scrape the JSON object containing earnings dates from Zacks.com
      val datePattern = """(?s)(\{\s*?"earnings_announcements_earnings_table"\s*?:\s*?\[\s*?\[.+?\]\s*?\])""".r
      val rawJson = datePattern findFirstIn rawHTML getOrElse (throw new Exception(s"could not parse earnings json from $url"))

      //add a closing } to the rawJson to make it parsable by the play JSON parser
      val json = Json.parse( rawJson + "}" )

      val jsonList:List[List[String]] = (json \\ "earnings_announcements_earnings_table")(0).as[List[List[String]]]
      jsonList.map { js => EarningsData( quote.toUpperCase(),
        LocalDate.parse( js(0), DateTimeFormatter.ofPattern("M/d/uuuu") ),
        YearMonth.parse( js(1), DateTimeFormatter.ofPattern("M/uuuu") ).getMonth.firstMonthOfQuarter().getValue / 3 + 1 )
      }
    } finally source.close()

  }

 
}
