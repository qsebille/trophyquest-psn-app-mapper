package com.trophyquest.builders

import com.trophyquest.utils.{PostgresReader, StringTransformUtils}
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.{DataFrame, SparkSession}

class TrophyCollectionBuilder(spark: SparkSession) {

  private final val postgresReader = new PostgresReader(spark)

  def build(): DataFrame = {
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

    titles
      .join(titlesTrophySets, "title_id")
      .join(trophySets, "trophy_set_id")
      .withColumn("title_name_slug", StringTransformUtils.toSlugUdf($"title_name"))
      .withColumn("game_uuid", StringTransformUtils.toUuidUdf($"title_name_slug"))
      .dropDuplicates("game_uuid", "trophy_set_id")
      .withColumn("trophy_collection_uuid", StringTransformUtils.toUuidUdf(concat_ws("_", $"trophy_set_id", $"game_uuid")))
      .select(
        $"trophy_collection_uuid",
        $"trophy_set_id",
        $"trophy_set_name",
        $"title_id",
        $"game_uuid",
        $"platform",
        $"icon_url"
      )
  }

}
