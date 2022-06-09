package net.jgp.books.spark.ch14.lab200_library_open

import org.apache.spark.sql.functions.{ col, call_udf, to_timestamp, udf, lit }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.types.{ DataType }
import java.sql.Timestamp
import com.github.mrpowers.spark.daria.sql.ColumnExt._

import net.jgp.books.spark.basic.Basic
import net.jgp.books.spark.utils.DateUtils

object OpenedLibraries extends Basic {
  def run(): Unit = {
    val spark = getSession("Custom UDF to check if in range")

    /*
    // too long
    spark.udf.register("isOpen", udf[Boolean, String, String, String, String, String, String, String, Timestamp](
      (hoursMon: String, hoursTue: String, hoursWed: String, hoursThu: String, hoursFri: String, 
          hoursSat: String, hoursSun: String, dateTime: Timestamp) => {
        DateUtils.isOpen(dateTime, hoursMon, hoursTue, hoursWed, hoursThu, hoursFri, hoursSat, hoursSun)
      }
    ))
    */
    // spark.udf.register("isOpen", udf[Boolean, String, String, String, String, String, String, String, Timestamp](DateUtils.isOpenUDF))

    // return (String, Int, Boolean)
    spark.udf.register("isOpen", udf(DateUtils.getDetailUDF _)) //, DataType.fromDDL("desc STRING, weekDay INT, opening BOOLEAN"))

    val librariesDF = getLibrariesDF(spark)

    val dateTimeDF = getDateTimeDF(spark)

    val df = librariesDF.crossJoin(dateTimeDF)
   
    import spark.implicits._
    // def call_udf(udfName: String, cols: Column*): Column
    val retDF = df.withColumn("open", call_udf("isOpen", 
        $"Opening_Hours_Monday", 
        $"Opening_Hours_Tuesday", 
        $"Opening_Hours_Wednesday", 
        $"Opening_Hours_Thursday", 
        $"Opening_Hours_Friday", 
        $"Opening_Hours_Saturday",
        lit("Closed"), 
        $"date")).
      drop("Opening_Hours_Monday").
      drop("Opening_Hours_Tuesday").
      drop("Opening_Hours_Wednesday").
      drop("Opening_Hours_Thursday").
      drop("Opening_Hours_Friday").
      drop("Opening_Hours_Saturday")

    retDF.printSchema
    //use [spark-daria](https://github.com/MrPowers/spark-daria) lib
    //finalDF.filter(col("open").isFalse).show(false)

    val finalDF = retDF.withColumn("desc", $"open._1").
      withColumn("weekDay", $"open._2").
      withColumn("opening", $"open._3").
      drop("open")

    finalDF.printSchema
    finalDF.filter(col("opening").isFalse).show(false)
    
  }

  private def getDateTimeDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    // *********************
    // old version
    // val rows = Seq("2020-06-02 08:36:19", "2021-02-08 15:36:19", "2022-06-08 19:36:19")
    // rows.toDS.toDF.withColumn("date", to_timestamp(col("value"))).drop("value")
    // *********************

    // Seq("2020-06-02 16:36:19", "2020-07-02 16:36:19", "2020-07-02 15:36:19").toDF("value").withColumn("date", to_timestamp(col("value"))).drop("value")
    
    // read from file
    val checkTimeDF = spark.read.format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load("data/south_dublin_libraries/check-times.csv")

    checkTimeDF.withColumn("date", to_timestamp(col("value"))).drop("value")
  }

  private def getLibrariesDF(spark: SparkSession): DataFrame = {
    val librariesDF = spark.read.
      format("csv").
      option("header", "true").
      option("inferSchema", "true").
      option("encoding", "cp1252").
      load("data/south_dublin_libraries/sdlibraries.csv").
      drop("Administrative_Authority").
      drop("Address1").
      drop("Address2").
      drop("Town").
      drop("Postcode").
      drop("County").
      drop("Phone").
      drop("Email").
      drop("Website").
      drop("Image").
      drop("WGS84_Latitude").
      drop("WGS84_Longitude")

    librariesDF.printSchema
    librariesDF.show(false)
    librariesDF
  }
}

