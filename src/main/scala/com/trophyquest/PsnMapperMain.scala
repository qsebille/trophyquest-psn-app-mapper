package com.trophyquest

import com.trophyquest.readers.PsnTitleReader
import org.apache.spark.sql.SparkSession

import java.time.LocalDate

object PsnMapperMain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TrophyQuest Transform")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val runDate: LocalDate = LocalDate.now()

    new PsnTitleReader(spark, runDate).read()


    spark.stop()
  }
}
