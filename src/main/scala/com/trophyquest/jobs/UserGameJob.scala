package com.trophyquest.jobs

import com.trophyquest.builders.{GameTrophyCollectionBuilder, UserProfileBuilder}
import com.trophyquest.config.JobConfig
import com.trophyquest.utils.{PostgresHelper, PostgresReader}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class UserGameJob(spark: SparkSession) {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  private final val postgresReader = new PostgresReader(spark)
  private final val postgresHelper = new PostgresHelper(spark)

  def run(): Unit = {
    import spark.implicits._

    val userProfiles: DataFrame = new UserProfileBuilder(spark).build()
      .select(
        $"user_uuid",
        $"user_psn_id",
      )
    logger.info("Loaded user profiles")

    val games: DataFrame = new GameTrophyCollectionBuilder(spark).build()
      .select(
        $"title_id",
        $"game_uuid",
      )
    logger.info("Loaded games")

    val userPlayedTitles: DataFrame = postgresReader.read("psn.user_played_title")
      .select(
        $"user_id" as "user_psn_id",
        $"title_id",
        $"last_played_at"
      )
    logger.info("Loaded user played titles")

    val userGames: DataFrame = userPlayedTitles
      .join(userProfiles, "user_psn_id")
      .join(games, "title_id")
      .select(
        $"user_uuid" as "user_id",
        $"game_uuid" as "game_id",
        $"last_played_at"
      )
      .dropDuplicates("user_id", "game_id")
      .persist()

    val count = userGames.count()
    logger.info(s"$count user played games computed")

    postgresHelper.truncate("app.user_game")

    userGames
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", JobConfig.postgres.url)
      .option("dbtable", "app.user_game")
      .option("user", JobConfig.postgres.user)
      .option("password", JobConfig.postgres.password)
      .option("driver", "org.postgresql.Driver")
      .option("stringtype", "unspecified")
      .option("batchsize", "5000")
      .save()

    userGames.unpersist()
  }

}
