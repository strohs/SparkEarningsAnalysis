package com.sae.datasource

import java.sql.Date
import java.time.LocalDate

/**
  * holds data related to a price movement between the start and end dates
  * @param start the date the move start
  * @param end the date the move ended
  * @param amount amount of the move
  */
case class PriceMove( start:Date, end:Date, amount:Double )
