package com.trophyquest.jobs

import com.trophyquest.builders.UserProfileBuilder
import com.trophyquest.config.JobConfig
import com.trophyquest.utils.{PostgresHelper, PostgresReader}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class UserProfileJob(spark: SparkSession) {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  private final val postgresReader = new PostgresReader(spark)
  private final val postgresHelper = new PostgresHelper(spark)

  def run(): Unit = {
    import spark.implicits._
    val userProfiles: DataFrame = new UserProfileBuilder(spark).build()
      .select(
        $"user_uuid" as "id",
        $"name",
        $"avatar_url",
      )
      .persist()

    val count = userProfiles.count()
    logger.info(s"$count profiles computed")

    postgresHelper.truncate("app.user_profile")

    userProfiles
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", JobConfig.postgres.url)
      .option("dbtable", "app.user_profile")
      .option("user", JobConfig.postgres.user)
      .option("password", JobConfig.postgres.password)
      .option("driver", "org.postgresql.Driver")
      .option("stringtype", "unspecified")
      .option("batchsize", "5000")
      .save()

    userProfiles.unpersist()
  }

}
