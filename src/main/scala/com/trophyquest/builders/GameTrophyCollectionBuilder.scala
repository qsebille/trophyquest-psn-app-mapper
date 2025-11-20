package com.trophyquest.builders

import com.trophyquest.utils.{PostgresReader, StringTransformUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

class GameTrophyCollectionBuilder(spark: SparkSession) {

  private final val postgresReader = new PostgresReader(spark)

  def build(): DataFrame = {
    import spark.implicits._
    val titles: DataFrame = postgresReader.read("psn.title").select(
      $"id" as "title_id",
      $"name" as "title_name",
      $"image_url",
    )
    val titlesTrophySets: DataFrame = postgresReader.read("psn.title_trophy_set").select(
      "title_id",
      "trophy_set_id",
    )
    val trophySets: DataFrame = postgresReader.read("psn.trophy_set").select(
      $"id" as "trophy_set_id",
      $"platform",
    )

    titles
      .join(titlesTrophySets, "title_id")
      .join(trophySets, "trophy_set_id")
      .withColumn("title_name_slug", StringTransformUtils.toSlugUdf($"title_name"))
      .withColumn("game_uuid", StringTransformUtils.toUuidUdf($"title_name_slug"))
      .select(
        $"game_uuid",
        $"title_id",
        $"trophy_set_id",
        $"title_name",
        $"platform",
        $"image_url"
      )
  }

}
