package com.soumya.df

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc, explode, split, trim, upper}

object WordCount {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("WordCount")
      .getOrCreate()

    val df = spark.read.text("in/word_count.text").toDF("words")
      .select(split(col("words"), " ").alias("words"))
      .select(explode(col("words")).as("words") )
      .select(upper(col("words")).as("words"))
      .persist()

    df.printSchema()

    val totalCnt: Long = df.count()
    println(s" Total count is $totalCnt")


    df
      .groupBy(col("words"))
      .count()
      .orderBy(desc("count"))
      .show(false)


  }


}
