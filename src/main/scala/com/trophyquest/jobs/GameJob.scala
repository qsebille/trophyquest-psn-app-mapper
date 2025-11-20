package com.trophyquest.jobs

import com.trophyquest.config.JobConfig
import com.trophyquest.utils.{PostgresHelper, PostgresReader, StringTransformUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class GameJob(spark: SparkSession) {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  private final val postgresReader = new PostgresReader(spark)
  private final val postgresHelper = new PostgresHelper(spark)

  def run(): Unit = {
    import spark.implicits._
    val rawTitles: DataFrame = postgresReader.read("psn.title")
    val rawTitlesTrophySets: DataFrame = postgresReader.read("psn.title_trophy_set")
    val rawTrophySets: DataFrame = postgresReader.read("psn.trophy_set")

    val titles: DataFrame = rawTitles
      .select("id", "name", "image_url")
      .withColumnRenamed("id", "title_id")
    val titlesTrophySets: DataFrame = rawTitlesTrophySets
      .select("title_id", "trophy_set_id")
    val trophySets: DataFrame = rawTrophySets
      .select("id", "platform")
      .withColumnRenamed("id", "trophy_set_id")

    val games = titles
      .join(titlesTrophySets, "title_id")
      .join(trophySets, "trophy_set_id")
      .withColumn("title_name_slug", StringTransformUtils.toSlugUdf($"name"))
      .withColumn("game_id", StringTransformUtils.toUuidUdf($"title_name_slug"))
      .orderBy("platform")
      .groupBy("game_id")
      .agg(
        first("image_url") as "image_url",
        first("name") as "title",
        concat_ws(",", sort_array(collect_set(col("platform")))) as "platforms"
      )
      .select(
        $"game_id" as "id",
        $"title",
        $"platforms",
        $"image_url"
      )

    val count = games.count()
    logger.info(s"$count application games computed")

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
      .save()

    games.unpersist()
  }

}
