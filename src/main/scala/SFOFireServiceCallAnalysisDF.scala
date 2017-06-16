package com.treselle.fscalls.analysis

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.functions._

object SFOFireServiceCallAnalysisDF {

  def main(args: Array[String]) {
    // SET THE LOG LEVEL TO ONLY PRINT ERRORS
    Logger.getLogger("org").setLevel(Level.ERROR)

    // CREATE SPARK SESSION 
    val spark = SparkSession.builder.getOrCreate()

    // CREATE SCHEMA
    val fireServiceCallSchema = StructType(Array(
      StructField("CallNumber", IntegerType, true), StructField("UnitID", StringType, true), StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true), StructField("CallDate", StringType, true), StructField("WatchDate", StringType, true),
      StructField("ReceivedDtTm", StringType, true), StructField("EntryDtTm", StringType, true), StructField("DispatchDtTm", StringType, true),
      StructField("ResponseDtTm", StringType, true), StructField("OnSceneDtTm", StringType, true), StructField("TransportDtTm", StringType, true),
      StructField("HospitalDtTm", StringType, true), StructField("CallFinalDisposition", StringType, true), StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true), StructField("City", StringType, true), StructField("ZipcodeOfIncident", IntegerType, true),
      StructField("Battalion", StringType, true), StructField("StationArea", StringType, true), StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true), StructField("Priority", StringType, true), StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true), StructField("CallTypeGroup", StringType, true), StructField("NumberOfAlarms", IntegerType, true), StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true), StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true), StructField("NeighborhooodsDistrict", StringType, true),
      StructField("Location", StringType, true), StructField("RowID", StringType, true)))

    // LOAD DATA
    val fireServiceCallRawDF = spark.read.format("csv").option("header", "true").schema(fireServiceCallSchema)
      .csv(args(0))

    // PRINT SCHEMA 
    fireServiceCallRawDF.printSchema()

    val fireServiceCallDateConvertedDF = fireServiceCallRawDF.withColumn("CallDateTS", from_unixtime(unix_timestamp(col("CallDate"), "MM/dd/yyyy"), "yyyy-MM-dd").cast("timestamp")).drop("CallDate")
	
    val fireServiceCallYearAddedDF = fireServiceCallDateConvertedDF.withColumn("CallYear", year(col("CallDateTS")))

	// FILTERING NEEDED COLUMN IN ADVANCE WILL REDUCE THE DATA MOVEMENT
	val fireServiceCallDF = fireServiceCallYearAddedDF.select("CallType", "NeighborhooodsDistrict","CallDateTS","CallYear")

	// RE ARRANGE NUMBER OF PARTITION
    fireServiceCallDF.cache().take(10)

    // PRINT SCHEMA 
    fireServiceCallDF.printSchema()

    // LOOK INTO TOP 20 ROWS IN THE DATA FILE
    fireServiceCallDF.show()

    // NUMBER OF RECORDS IN THE FILE
    val totalRecords = fireServiceCallDF.count()
    println(s"Number of records in the data file: $totalRecords")

    // Q1: HOW MANY DIFFERENT TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?
    println(s"Q1: HOW MANY DIFFERENT TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?")
    val distinctTypesOfCallsDF = fireServiceCallDF.select("CallType").distinct()
    distinctTypesOfCallsDF.collect().foreach(println)

    // Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?
    println(s"Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?")
    val distinctTypesOfCallsSortedDF = fireServiceCallDF.select("CallType").groupBy("CallType").count().orderBy(desc("count"))
    distinctTypesOfCallsSortedDF.collect().foreach(println)

    // Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?
    println(s"Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?")
    val fireServiceCallYearsDF = fireServiceCallDF.select("CallYear").groupBy("CallYear").count().orderBy(desc("count"))
    fireServiceCallYearsDF.show()

    // Q4: HOW MANY SERVICE CALLS WERE LOGGED IN THE PAST 7 DAYS?
    println(s"Q4: HOW MANY SERVICE CALLS WERE LOGGED IN THE PAST 7 DAYS?")
    val last7DaysServiceCallDF = fireServiceCallDF.select("CallDateTS").groupBy("CallDateTS").count().orderBy(desc("CallDateTS"))
    last7DaysServiceCallDF.show(7)

    // Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?
    println(s"Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?")
    val neighborhoodDistrictCallsDF = fireServiceCallDF.filter("CallYear == 2016").select("NeighborhooodsDistrict").groupBy("NeighborhooodsDistrict").count().orderBy(desc("count"))
    neighborhoodDistrictCallsDF.collect().foreach(println)
  }
}