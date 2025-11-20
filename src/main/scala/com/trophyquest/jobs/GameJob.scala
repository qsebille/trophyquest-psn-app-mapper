package com.trophyquest.jobs

import com.trophyquest.builders.GameTrophyCollectionBuilder
import com.trophyquest.config.JobConfig
import com.trophyquest.utils.PostgresHelper
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{collect_set, concat_ws, first, sort_array}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class GameJob(spark: SparkSession) {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  private final val postgresHelper = new PostgresHelper(spark)

  def run(): Unit = {
    import spark.implicits._

    val games: DataFrame = new GameTrophyCollectionBuilder(spark).build()
      .groupBy("game_uuid")
      .agg(
        first("image_url") as "image_url",
        first("title_name") as "title_name",
        concat_ws(",", sort_array(collect_set($"platform"))) as "platforms"
      )
      .select(
        $"game_uuid" as "id",
        $"title_name" as "title",
        $"platforms",
        $"image_url"
      )
      .persist()

    val count = games.count()
    logger.info(s"$count games computed")

    postgresHelper.truncate("app.game")

    games
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", JobConfig.postgres.url)
      .option("dbtable", "app.game")
      .option("user", JobConfig.postgres.user)
      .option("password", JobConfig.postgres.password)
      .option("driver", "org.postgresql.Driver")
      .option("stringtype", "unspecified")
      .option("batchsize", "5000")
      .save()

    games.unpersist()
  }

}
