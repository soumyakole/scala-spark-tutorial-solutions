package com.soumya.df

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{DecimalType, IntegerType}

object AverageHousePriceSolution {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("AverageHousePriceSolution")
      .master("local[2]")
      .getOrCreate()

    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .csv("in/RealEstate.csv")
      .select("Price", "Bedrooms")
      .withColumn("Price", col("Price").cast(new DecimalType(38, 0)))
      .withColumn("Bedrooms", col("Bedrooms").cast(IntegerType))
      .persist()

    //Dataframe API
    val retDf = df.groupBy("Bedrooms")
      .avg("Price")
      .withColumnRenamed("avg(Price)", "avg")
      .orderBy(desc("Bedrooms"))

    retDf.show(false)

    //SQL
    df.createOrReplaceTempView("myTable")
    val retDf2 = spark.sql("select Bedrooms, avg(Price) as avg from myTable group by Bedrooms order by Bedrooms desc")
    retDf2.show(false)

  }

}
