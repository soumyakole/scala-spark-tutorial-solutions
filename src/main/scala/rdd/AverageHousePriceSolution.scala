package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, IntegerType}

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

    val tmpDf = df
      .select("Price", "Bedrooms")
      .withColumn("Price", col("Price").cast(new DecimalType(38,0)))
      .withColumn("Bedrooms", col("Bedrooms").cast(IntegerType))

    val retDf = tmpDf.groupBy("Bedrooms")
      .avg("Price")
      .withColumnRenamed("avg(Price)", "average")
      .orderBy(desc("Bedrooms"))

    retDf.show(false)

    tmpDf.createOrReplaceTempView("myTable")
    val retDf2 = spark.sql("select Bedrooms, avg(Price) from myTable group by Bedrooms order by Bedrooms desc")
    retDf2.show(false)

  }

}
