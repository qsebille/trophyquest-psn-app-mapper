package com.trophyquest.jobs

import com.trophyquest.builders.TrophyBuilder
import com.trophyquest.config.JobConfig
import com.trophyquest.utils.PostgresHelper
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class TrophyJob(spark: SparkSession) {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  private final val postgresHelper = new PostgresHelper(spark)

  def run(): Unit = {
    import spark.implicits._

    val trophies: DataFrame = new TrophyBuilder(spark).build().select(
        $"trophy_uuid" as "id",
        $"trophy_collection_uuid" as "trophy_collection_id",
        $"game_group_id",
        $"rank",
        $"title",
        $"detail" as "description",
        $"trophy_type",
        $"is_hidden",
        $"icon_url",
      )
      .persist()

    val count = trophies.count()
    logger.info(s"$count trophies computed")

    postgresHelper.truncate("app.trophy")

    trophies
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", JobConfig.postgres.url)
      .option("dbtable", "app.trophy")
      .option("user", JobConfig.postgres.user)
      .option("password", JobConfig.postgres.password)
      .option("driver", "org.postgresql.Driver")
      .option("stringtype", "unspecified")
      .option("batchsize", "5000")
      .save()

    trophies.unpersist()
  }

}
