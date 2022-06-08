package net.jgp.books.spark.ch14.lab200_library_open

import org.apache.spark.sql.functions.{col, call_udf, to_timestamp, udf, lit}
import org.apache.spark.sql.{ DataFrame, SparkSession }
import java.sql.Timestamp

import net.jgp.books.spark.basic.Basic

object OpenedLibraries extends Basic {
  def run(): Unit = {
    val spark = getSession("Custom UDF to check if in range")

    spark.udf.register("isOpen", udf[Boolean, String, String, String, String, String, String, String, Timestamp](
      (hoursMon: String, hoursTue: String, hoursWed: String, hoursThu: String, hoursFri: String, 
          hoursSat: String, hoursSun: String, dateTime: Timestamp) => {
        true
      }
    ))

    val librariesDF = getLibrariesDF(spark)

    val dateTimeDF = getDateTimeDF(spark)

    val df = librariesDF.crossJoin(dateTimeDF)
    df.show()
   
    import spark.implicits._
    // def call_udf(udfName: String, cols: Column*): Column
    val finalDF = df.withColumn("open", call_udf("isOpen", 
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

    finalDF.printSchema
    finalDF.show()
  }

  private def getDateTimeDF(spark: SparkSession): DataFrame = {
    val rows = Seq("2020-06-02 08:36:19", "2021-02-08 15:36:19", "2022-06-08 19:36:19")

    import spark.implicits._
    val dateTimeDf = rows.toDS.toDF.withColumn("date", to_timestamp(col("value"))).drop("value")
    dateTimeDf.printSchema
    dateTimeDf
  }

  private def getLibrariesDF(spark: SparkSession): DataFrame = {
    val librariesDF = spark.read.
      format("csv").
      option("header", "true").
      option("inferSchema", "true").
      option("encoding", "cp1252").
      load("data/south_dublin_libraries/sdlibraries.csv")

    librariesDF.printSchema

    librariesDF.
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
  }
}

