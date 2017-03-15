package com.sae.datasource

import java.sql.Date


/**
  * Created with IntelliJ IDEA.
  * User: Cliff
  * Date: 2/21/2017
  * Time: 2:55 PM
  */
case class PriceData( quote:String, date:Date, close:Double, adjClose:Double )



object PriceData {

  //ordering for Java SQL Dates
  implicit val localDateOrdering: Ordering[Date] = Ordering.by( _.toLocalDate.toEpochDay )

  def orderingByClosingPrice[A <: PriceData]: Ordering[PriceData] =
    Ordering.by( pd => pd.close )

  def orderingByDate[A <: PriceData]: Ordering[PriceData] =
    Ordering.by( pd => pd.date )

}
