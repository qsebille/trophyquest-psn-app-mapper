package com.trophyquest.utils

import com.trophyquest.config.JobConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager

class PostgresHelper(spark: SparkSession) {
  private final val logger: Logger = Logger.getLogger(this.getClass)

  def truncate(tableName: String): Unit = {
    val connection = DriverManager.getConnection(JobConfig.postgres.url, JobConfig.postgres.user, JobConfig.postgres.password)
    try {
      val statement = connection.createStatement()
      try {
        statement.executeUpdate(s"TRUNCATE TABLE $tableName CASCADE")
        logger.info(s"Truncate table $tableName: success")
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

}
