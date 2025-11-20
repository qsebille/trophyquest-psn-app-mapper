package com.trophyquest.jobs

import com.trophyquest.config.JobConfig
import com.trophyquest.utils.{PostgresHelper, PostgresReader, StringTransformUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class UserProfileJob(spark: SparkSession) {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  private final val postgresReader = new PostgresReader(spark)
  private final val postgresHelper = new PostgresHelper(spark)

  def run(): Unit = {
    import spark.implicits._
    val userProfiles: DataFrame = postgresReader.read("psn.user_profile")

    val appProfiles = userProfiles
      .withColumn("id", StringTransformUtils.toUuidUdf($"id"))
      .drop("created_at")
      .persist()

    val count = appProfiles.count()
    logger.info(s"$count application profiles computed")

    postgresHelper.truncate("app.user_profile")

    appProfiles
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", JobConfig.postgres.url)
      .option("dbtable", "app.user_profile")
      .option("user", JobConfig.postgres.user)
      .option("password", JobConfig.postgres.password)
      .option("driver", "org.postgresql.Driver")
      .option("stringtype", "unspecified")
      .save()

    appProfiles.unpersist()
  }

}
