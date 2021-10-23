package com.soumya.df

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType

object AirportsByLatitudeSolution {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[2]").appName("AirportLatitude")
      .getOrCreate()

    val input = sparkSession.read.csv("in/airports.text")
    val castDF = input
      .withColumn("latitude", col("_c6").cast(new DecimalType(38, 2)))
      .drop(col("_c6"))
      .filter(col("latitude") > 40)
      .select(col("_c1"), col("latitude"))
    castDF.write
      .mode("overwrite")
      .csv("out/airports_in_usa.text")

  }

}
