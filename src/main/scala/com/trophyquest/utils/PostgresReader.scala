package com.trophyquest.utils

import com.trophyquest.config.JobConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

class PostgresReader(spark: SparkSession) {
  private final val logger: Logger = Logger.getLogger(this.getClass)

  def read(tableName: String): DataFrame = {
    val url: String = JobConfig.postgres.url
    logger.info(s"Reading postgres table $tableName in: $url")

    spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", tableName)
      .option("user", JobConfig.postgres.user)
      .option("password", JobConfig.postgres.password)
      .option("driver", "org.postgresql.Driver")
      .load()
  }

}
