package com.treselle.fscalls.analysis

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.sql.Date
import org.apache.spark.SparkConf

object FireServiceCallAnalysisSerialized {
  /**
   * Convert to year which will extract the year from Call Date column.
   */
  def convertToYear(columns: Array[String]): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val utilDate = simpleDateFormat.parse(columns(4))
    new SimpleDateFormat("yyyy").format(utilDate)
  }

  /**
   * Convert Call Date from String into Date.
   */
  def convertToDate(columns: Array[String]): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val utilDate = simpleDateFormat.parse(columns(4))
    new SimpleDateFormat("yyyy-MM-dd").format(utilDate)
  }

  def main(args: Array[String]) {
    // SET THE LOG LEVEL TO ONLY PRINT ERRORS
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // CREATE SPARK SESSION 
    val spark = SparkSession.builder.config(sparkConfig).getOrCreate()

    // LOAD DATA 
    val fireServiceCallRawRDD = spark.sparkContext.textFile(args(0))

    // EXTRACT THE HEADER
    val header = fireServiceCallRawRDD.first()

    // FILTER THE HEADER ROW AND SPLIT THE COLUMNS IN THE DATA FILE (EXCLUDE COMMA WITH IN DOUBLE QUOTES)
    val filteredFireServiceCallRDD = fireServiceCallRawRDD.filter(row => row != header).map(x => x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))

    // CACHE/PERSIST THE RDD
    filteredFireServiceCallRDD.setName("FireServiceCallSerializedRDD").persist().take(10)

    // NUMBER OF RECORDS IN THE FILE
    val totalRecords = filteredFireServiceCallRDD.count()
    println(s"Number of records in the data file: $totalRecords")

    // Q1: HOW MANY DIFFERENT TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?
    println(s"Q1: HOW MANY DIFFERENT TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?")
    val distinctTypesOfCallsRDD = filteredFireServiceCallRDD.map(x => x(3))
    distinctTypesOfCallsRDD.distinct().collect().foreach(println)

    // Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?
    println(s"Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?")
    val distinctTypesOfCallsSortedRDD = distinctTypesOfCallsRDD.map(x => (x, 1)).reduceByKey((x, y) => (x + y)).map(x => (x._2, x._1)).sortByKey(false)
    distinctTypesOfCallsSortedRDD.collect().foreach(println)

    // Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?
    println(s"Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?")
    val fireServiceCallYearsRDD = filteredFireServiceCallRDD.map(convertToYear).map(x => (x, 1)).reduceByKey((x, y) => (x + y)).map(x => (x._2, x._1)).sortByKey(false)
    fireServiceCallYearsRDD.take(20).foreach(println)

    // Q4: HOW MANY SERVICE CALLS WERE LOGGED IN THE PAST 7 DAYS?
    println(s"Q4: HOW MANY SERVICE CALLS WERE LOGGED IN THE PAST 7 DAYS?")
    val last7DaysServiceCallRDD = filteredFireServiceCallRDD.map(convertToDate).map(x => (x, 1)).reduceByKey((x, y) => (x + y)).sortByKey(false)
    last7DaysServiceCallRDD.take(7).foreach(println)

    // Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR? 
    println(s"Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?")
    val neighborhoodDistrictCallsRDD = filteredFireServiceCallRDD.filter(row => (convertToYear(row) == "2016")).map(x => x(31)).map(x => (x, 1)).reduceByKey((x, y) => (x + y)).map(x => (x._2, x._1)).sortByKey(false)
    neighborhoodDistrictCallsRDD.collect().foreach(println)
  }
}