package com.trophyquest.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

class PsnTitleReader(
                      spark: SparkSession,
                      runDate: LocalDate,
                    ) {

  def read(): DataFrame = {
    val jdbcUrl = "jdbc:postgresql://localhost:5432/trophyquest"
    val dbUser = "postgres"
    val dbPass = "postgres"

    val psnTitlesDf: DataFrame = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "psn.title")
      .option("user", dbUser)
      .option("password", dbPass)
      .option("driver", "org.postgresql.Driver")
      .load()

    psnTitlesDf.show(10, truncate = false)

    psnTitlesDf
  }

}
