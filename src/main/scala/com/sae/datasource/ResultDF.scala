package com.sae.datasource

import java.sql.Date

/**
  * convenience case class for exporting results of the Earnings Analysis to a DataFrame
  */

// qtr, relDate, bMoveStart, bMoveEnd, bMove, aMoveStart, aMoveEnd, aMove
case class ResultDF(qtr:Int, relDate:Date,
                    bMoveStart:Date, bMoveEnd:Date, bMove:Double,
                    aMoveStart:Date, aMoveEnd:Date, aMove:Double)
