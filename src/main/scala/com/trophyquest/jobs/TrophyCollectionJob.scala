package com.trophyquest.jobs

import com.trophyquest.config.JobConfig
import com.trophyquest.utils.{PostgresHelper, PostgresReader, StringTransformUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class TrophyCollectionJob(spark: SparkSession) {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  private final val postgresReader = new PostgresReader(spark)
  private final val postgresHelper = new PostgresHelper(spark)

  def run(): Unit = {
    import spark.implicits._
    val titles: DataFrame = postgresReader.read("psn.title").select(
      $"id" as "title_id",
      $"name" as "title_name",
    )
    val titlesTrophySets: DataFrame = postgresReader.read("psn.title_trophy_set")
      .select(
        "title_id",
        "trophy_set_id",
      )
    val trophySets: DataFrame = postgresReader.read("psn.trophy_set").select(
      $"id" as "trophy_set_id",
      $"name" as "trophy_set_name",
      $"platform",
      $"icon_url",
    )

    val trophyCollections: DataFrame = titles
      .join(titlesTrophySets, "title_id")
      .join(trophySets, "trophy_set_id")
      .withColumn("title_name_slug", StringTransformUtils.toSlugUdf($"title_name"))
      .withColumn("game_id", StringTransformUtils.toUuidUdf($"title_name_slug"))
      .withColumn("trophy_collection_id", StringTransformUtils.toUuidUdf(concat_ws("_", $"trophy_set_id", $"game_id")))
      .select(
        $"trophy_collection_id" as "id",
        $"game_id",
        $"trophy_set_name" as "title",
        $"platform",
        $"icon_url" as "image_url"
      )
      .dropDuplicates("game_id", "id")
      .persist()

    val count = trophyCollections.count()
    logger.info(s"$count application trophy collections computed")

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
      .save()

    trophyCollections.unpersist()
  }

}
