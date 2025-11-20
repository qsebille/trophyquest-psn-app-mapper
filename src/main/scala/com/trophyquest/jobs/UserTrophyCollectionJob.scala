package com.trophyquest.jobs

import com.trophyquest.builders.{TrophyCollectionBuilder, UserProfileBuilder}
import com.trophyquest.config.JobConfig
import com.trophyquest.utils.{PostgresHelper, PostgresReader}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class UserTrophyCollectionJob(spark: SparkSession) {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  private final val postgresReader = new PostgresReader(spark)
  private final val postgresHelper = new PostgresHelper(spark)

  def run(): Unit = {
    import spark.implicits._

    val userPlayedTitle: DataFrame = postgresReader.read("psn.user_played_title")
      .select($"user_id" as "user_psn_id", $"title_id")
    logger.info("Loaded user played titles")

    val userProfiles: DataFrame = new UserProfileBuilder(spark).build()
      .select($"user_uuid", $"user_psn_id")
    logger.info("Loaded user profiles")

    val trophyCollections: DataFrame = new TrophyCollectionBuilder(spark).build()
      .select($"title_id", $"trophy_collection_uuid")
    logger.info("Loaded trophy collections")

    val userTrophyCollections: DataFrame = userPlayedTitle
      .join(userProfiles, "user_psn_id")
      .join(trophyCollections, "title_id")
      .select(
        $"user_uuid" as "user_id",
        $"trophy_collection_uuid" as "trophy_collection_id",
      )
      .dropDuplicates()
      .persist()

    val count = userTrophyCollections.count()
    logger.info(s"$count user played trophy collections computed")

    postgresHelper.truncate("app.user_trophy_collection")

    userTrophyCollections
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", JobConfig.postgres.url)
      .option("dbtable", "app.user_trophy_collection")
      .option("user", JobConfig.postgres.user)
      .option("password", JobConfig.postgres.password)
      .option("driver", "org.postgresql.Driver")
      .option("stringtype", "unspecified")
      .option("batchsize", "5000")
      .save()

    userTrophyCollections.unpersist()
  }

}
