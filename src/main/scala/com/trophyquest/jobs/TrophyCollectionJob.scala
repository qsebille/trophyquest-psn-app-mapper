package com.trophyquest.jobs

import com.trophyquest.builders.TrophyCollectionBuilder
import com.trophyquest.config.JobConfig
import com.trophyquest.utils.PostgresHelper
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class TrophyCollectionJob(spark: SparkSession) {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  private final val postgresHelper = new PostgresHelper(spark)

  def run(): Unit = {
    import spark.implicits._

    val trophyCollections: DataFrame = new TrophyCollectionBuilder(spark).build()
      .select(
        $"trophy_collection_uuid" as "id",
        $"game_uuid" as "game_id",
        $"trophy_set_name" as "title",
        $"platform",
        $"icon_url" as "image_url"
      )
      .persist()

    val count = trophyCollections.count()
    logger.info(s"$count trophy collections computed")

    postgresHelper.truncate("app.trophy_collection")

    trophyCollections
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", JobConfig.postgres.url)
      .option("dbtable", "app.trophy_collection")
      .option("user", JobConfig.postgres.user)
      .option("password", JobConfig.postgres.password)
      .option("driver", "org.postgresql.Driver")
      .option("stringtype", "unspecified")
      .option("batchsize", "5000")
      .save()

    trophyCollections.unpersist()
  }

}
