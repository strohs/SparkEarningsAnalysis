# Spark Earnings Analysis
A spark app that I used to learn the fundamentals of the Apache Spark API.
  
The overall goal of the app is to find the maximum price move a company's stock makes (historically, in each quarter) before and 
after an earnings release date. Stock Price closing data is retrieved for the last four years from Yahoo Finance. Earnings release date information 
is pulled for the last four years from Zacks.com website.

## Installation
Clone this project and then use the "sbt assembly" command to build the application jar file. The jar can then be
 submitted to your spark cluster. See below

## Usage
There are two versions of the app:
* EarningsTrendDS - uses the spark dataset API to find the max price move for each quarter and then outputs the results
to a .csv file. Then csv file can then be imported into the R programming environment and graphed. A sample R file to 
do this is located at src/main/R/earningsRelease.R
* EarningsTrendRDD - uses Spark's RDD API to compute max price moves (just like EarningsTrendDS) but the results are
output to standard out rather than a .csv file

The only parameter required for each app is the ticker symbol of the company you want to examine. For example, to find 
price moves for WalMART (ticker: WMT):
```bash
./bin/spark-submit \
  --class com.sae.EarningsTrendDS \
  --master local[8] \
  /path/to/SparkEarningsAnalysis.jar \
  WMT
  ```
EarningsTrendDS is the preferred version, as it used the newer Spark APIs and outputs its results into a csv format.

## Examples
Here is an example output from running EarningsTrendDS ticker symbol WMT:
```csv
qtr,relDate,bMoveStart,bMoveEnd,bMove,aMoveStart,aMoveEnd,aMove
1,2013-02-21,2013-01-24,2013-02-21,0.0,2013-03-13,2013-03-18,1.4000020000000006
1,2014-02-20,2014-02-03,2014-02-14,3.129997000000003,2014-02-21,2014-03-12,2.4099960000000067
1,2015-02-19,2015-01-26,2015-01-30,3.6499940000000066,2015-02-23,2015-03-11,3.9099959999999925
1,2016-02-18,2016-01-22,2016-02-01,4.810001,2016-02-19,2016-03-15,3.4299919999999986
2,2013-05-16,2013-04-30,2013-05-15,2.1400000000000006,2013-05-17,2013-05-31,3.0300069999999977
2,2014-05-15,2014-04-22,2014-04-28,2.200004000000007,2014-05-22,2014-06-05,1.93000099999999
2,2015-05-19,2015-05-06,2015-05-18,2.269996000000006,2015-05-21,2015-06-15,4.18000099999999
2,2016-05-19,2016-04-25,2016-05-18,6.319998999999996,2016-05-23,2016-06-08,1.7799990000000037
3,2013-08-15,2013-08-05,2013-08-14,2.369995000000003,2013-08-16,2013-08-28,1.7300039999999939
3,2014-08-14,2014-07-18,2014-08-05,3.75,2014-08-15,2014-09-05,3.6099999999999994
3,2015-08-18,2015-08-05,2015-08-07,2.260002,2015-08-19,2015-08-25,5.470001999999994
3,2016-08-18,2016-08-10,2016-08-16,1.0599979999999931,2016-09-06,2016-09-09,2.699996999999996
4,2013-11-14,2013-10-21,2013-11-11,3.8599999999999994,2013-11-21,2013-12-03,2.3499979999999994
4,2014-11-13,2014-10-17,2014-11-10,5.3400040000000075,2014-11-14,2014-11-28,4.5800020000000075
4,2015-11-17,2015-10-22,2015-11-13,2.480004000000001,2015-11-18,2015-12-02,2.5800020000000004
4,2016-11-17,2016-10-21,2016-11-15,3.0800020000000075,2016-11-18,2016-12-13,3.260002
```
the Columns are as follows:
* qtr - the quarter of the earnings release, this will be in Integer between 1 and 4
* relDate - the earnings release date in yyyy-mm-dd format
* bMoveStart - the start date of the maximum price move before earnings were released
* bMoveEnd - the end date of the maximum price move after earnings were released
* bMove - the dollar amount of the move that occured between bMoveStart and bMoveEnd
* aMoveStart,aMoveEnd,aMove - exactly like the bMove* data except these are for four weeks after the earnings release 


### Bugs 
Code is provided as is. This was mainly a learning experience in order to better understand the Spark API(s)

## License
Distributed under the Eclipse Public License