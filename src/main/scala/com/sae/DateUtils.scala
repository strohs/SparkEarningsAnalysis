package com.sae

import java.time.{LocalDate, Period}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit


/**
  * functions for computing dates and date offsets. Makes heavy use of java.time classes
  * User: Cliff
  * Date: 2/24/2017
  * Time: 3:25 PM
  */
object DateUtils {


  def dateTest() : Unit = {
    val ds1 = "2-01-2017"
    val ds2 = "2-15-2017"
    val ds3 = "2017-03-19"
    val ds4 = "2017-03-19"
    val d1 = LocalDate.parse( ds1, DateTimeFormatter.ofPattern("M-d-uuuu") )
    val d2 = LocalDate.parse( ds2, DateTimeFormatter.ofPattern("M-d-uuuu") )
    val d3 = LocalDate.parse( ds3 )
    val d4 = LocalDate.parse( ds4 )

    val dates:List[LocalDate] = List( d2, d3, d4 ,d1 )
    println( dates.sortWith( (d1,d2) => d1.isBefore(d2) ) )
    println( s"$d1   $d2")
    println( d1.isBefore(d2) )

    println( d1.plusWeeks(3) )
    println( d2.until( d1 ).getDays )
    println( d1.until(d2, ChronoUnit.DAYS ) )

    println( Period.between( d1, d2 ).getDays )

  }


  def dateBetween( date:LocalDate, begin:LocalDate, end:LocalDate ): Boolean = {
    date.isAfter( begin ) && date.isBefore( end )
  }
}
