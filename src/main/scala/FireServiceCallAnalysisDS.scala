package com.treselle.fscalls.analysis

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DateType
import java.sql.Timestamp
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.functions._

object FireServiceCallAnalysisDS {

  // FIRES SERVICE CALL CLASS FOR TYPE SAFTY
  case class fireServiceCall(CallNumber: java.lang.Integer, UnitID: String, IncidentNumber: java.lang.Integer, CallType: String, CallDate: String,
                             WatchDate: String, ReceivedDtTm: String, EntryDtTm: String, DispatchDtTm: String, ResponseDtTm: String,
                             OnSceneDtTm: String, TransportDtTm: String, HospitalDtTm: String, CallFinalDisposition: String,
                             AvailableDtTm: String, Address: String, City: String, ZipcodeOfIncident: Integer, Battalion: String,
                             StationArea: String, Box: String, OriginalPriority: String, Priority: String, FinalPriority: java.lang.Integer,
                             ALSUnit: String, CallTypeGroup: String, NumberOfAlarms: java.lang.Integer, UnitType: String, UnitSequenceInCallDispatch: java.lang.Integer,
                             FirePreventionDistrict: String, SupervisorDistrict: String, NeighborhooodsDistrict: String,
                             Location: String, RowID: String, CallDateTS: java.sql.Timestamp, CallYear: java.lang.Integer)

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
    import spark.implicits._
    val fireServiceCallDS = spark.read.format("csv").option("header", "true").schema(fireServiceCallSchema)
      .csv(args(0))
      .withColumn("CallDateTS", from_unixtime(unix_timestamp(col("CallDate"), "MM/dd/yyyy"), "yyyy-MM-dd").cast("timestamp"))
      .withColumn("CallYear", year(col("CallDateTS")))
      .as[fireServiceCall]

    // CACHE/PERSIST THE DATASET
    fireServiceCallDS.persist().take(10)

    // PRINT SCHEMA
    fireServiceCallDS.printSchema()

    // LOOK INTO TOP 20 ROWS IN THE DATA FILE
    fireServiceCallDS.show()

    // NUMBER OF RECORDS IN THE FILE
    val totalRecords = fireServiceCallDS.count()
    println(s"Number of records in the data file: $totalRecords")

    // Q1: HOW MANY DIFFERENT TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?
    println(s"Q1: HOW MANY DIFFERENT TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?")
    val distinctTypesOfCallsDS = fireServiceCallDS.select(col("CallType"))
    distinctTypesOfCallsDS.distinct().collect().foreach(println)
    //    
    // Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?
    println(s"Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?")
    val distinctTypesOfCallsSortedDS = fireServiceCallDS.select(col("CallType")).groupBy(col("CallType")).agg(count(col("CallType")).alias("count")).orderBy(desc("count"))
    distinctTypesOfCallsSortedDS.collect().foreach(println)

    // Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?
    println(s"Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?")
    val fireServiceCallYearsDS = fireServiceCallDS.select(col("CallYear")).groupBy(col("CallYear")).agg(count(col("CallYear")).alias("count")).orderBy(desc("count"))
    fireServiceCallYearsDS.show()

    // Q4: HOW MANY SERVICE CALLS WERE LOGGED IN THE PAST 7 DAYS?
    println(s"Q4: HOW MANY SERVICE CALLS WERE LOGGED IN THE PAST 7 DAYS?")
    val last7DaysServiceCallDS = fireServiceCallDS.select(col("CallDateTS")).groupBy(col("CallDateTS")).agg(count(col("CallDateTS")).alias("count")).orderBy(desc("CallDateTS"))
    last7DaysServiceCallDS.show(7)

    // Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?
    println(s"Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?")
    val neighborhoodDistrictCallsDS = fireServiceCallDS.filter(fireServiceCall => fireServiceCall.CallYear == 2016).select(col("NeighborhooodsDistrict")).groupBy(col("NeighborhooodsDistrict")).agg(count(col("NeighborhooodsDistrict")).alias("count")).orderBy(desc("count"))
    neighborhoodDistrictCallsDS.collect().foreach(println)
  }
}