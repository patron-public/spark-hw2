package com.epam.spark.training.dataframes

import com.epam.spark.training.dataframes.model.{Airport, Carrier, Flight}
import org.apache.spark.{SparkConf, SparkContext}

object CsvFlightsProcessor {
  val appName:String = "Server Log Analyzer"
  val master:String = "local"
  var sc:SparkContext = _
  var sqlContext = _

  def readFiles() = {

    val airports = sc.textFile("/training/spark/hw2/data/airports.csv")
      .filter(header => !header.startsWith(""""iata""""))
      .map(_.split("(?:^\")|(?:(\")?[,](?=[\\d-]))|(?:\",\")"))
      .filter(arr => arr.length==8) //NA= not USA
      .map(p => Airport(p(1), p(2), p(3), p(4), p(5), p(6).toDouble, p(7).toDouble))
      .toDF()

    airports.registerTempTable("airports")

    val carriers = sc.textFile("/training/spark/hw2/data/carriers.csv")
      .filter(header => !header.equals("Code,Description"))
      .map(_.split("(?:^\")|(?:[\"][,][\"])|(?:\"$)"))
      .map(p => Carrier(p(1), p(2)))
      .toDF()

    carriers.registerTempTable("carriers")

    val flightsTextArrRDD = sc.textFile("/training/spark/hw2/data/2007.csv")
      .filter(hdr => !hdr.startsWith("Year"))
      .map(_.split(","))

    val flights = flightsTextArrRDD
      .map(p => Flight(p(8), p(16), p(17), p(21).equals("1")))
      .toDF()

    flights.registerTempTable("flights")

  }

  def main(args: Array[String]) {

    if (args.length != 2){
      print("Please, use 2 parameters: [source file, dest file]")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    sc = new SparkContext(conf)
    sqlContext = new org.apache.spark.sql.SQLContext(sc)

    readFiles()
//    sqlContext.read.text("/tarining/spark/hw2/data/airports.csv")
    val dataSet = sc.textFile(args(0))



    dataSet.saveAsTextFile("hdfs://"+ args(1))
  }


}
