package rdd.nasaApacheWebLogs

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SameHostsSolution {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def loadCsv(filewithPath: String)(implicit spark: SparkSession) : DataFrame ={
    spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .load(filewithPath)
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("SameHostsSolution")
      .getOrCreate()

    val df1 = loadCsv("in/nasa_19950701.tsv").select(col("host"))

    val df2 = loadCsv("in/nasa_19950801.tsv").select(col("host"))

    val resultDF = df1.intersect(df2).repartition(2)

    print(resultDF.rdd.partitions.length)


    resultDF
      .write
      .mode("overwrite")
      .csv("out/nasa_logs_same_hosts.csv")

  }

}
