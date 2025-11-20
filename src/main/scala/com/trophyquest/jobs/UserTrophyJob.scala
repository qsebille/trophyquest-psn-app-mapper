package com.trophyquest.jobs

import com.trophyquest.builders.{TrophyBuilder, UserProfileBuilder}
import com.trophyquest.config.JobConfig
import com.trophyquest.utils.{PostgresHelper, PostgresReader}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class UserTrophyJob(spark: SparkSession) {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  private final val postgresReader = new PostgresReader(spark)
  private final val postgresHelper = new PostgresHelper(spark)

  def run(): Unit = {
    import spark.implicits._

    val users: DataFrame = new UserProfileBuilder(spark).build()
      .select($"user_psn_id", $"user_uuid")
    logger.info("Loaded users")

    val trophies: DataFrame = new TrophyBuilder(spark).build()
      .select($"trophy_id", $"trophy_uuid")
    logger.info("Loaded trophies")

    val userEarnedTrophies: DataFrame = postgresReader.read("psn.user_earned_trophy")
      .select($"user_id" as "user_psn_id", $"trophy_id", $"earned_at")
    logger.info("Loaded user earned trophies")

    val userTrophies: DataFrame = users
      .join(userEarnedTrophies, "user_psn_id")
      .join(trophies, "trophy_id")
      .select($"user_uuid" as "user_id", $"trophy_uuid" as "trophy_id", $"earned_at")
      .dropDuplicates()
      .persist()

    val count = userTrophies.count()
    logger.info(s"$count user earned trophies computed")

    postgresHelper.truncate("app.user_trophy")

    userTrophies
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", JobConfig.postgres.url)
      .option("dbtable", "app.user_trophy")
      .option("user", JobConfig.postgres.user)
      .option("password", JobConfig.postgres.password)
      .option("driver", "org.postgresql.Driver")
      .option("stringtype", "unspecified")
      .option("batchsize", "5000")
      .save()

    userTrophies.unpersist()
  }

}
