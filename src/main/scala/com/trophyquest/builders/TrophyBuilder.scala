package com.trophyquest.builders

import com.trophyquest.utils.{PostgresReader, StringTransformUtils}
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.{DataFrame, SparkSession}

class TrophyBuilder(spark: SparkSession) {

  private final val postgresReader = new PostgresReader(spark)

  def build(): DataFrame = {
    import spark.implicits._

    val trophyCollections: DataFrame = new TrophyCollectionBuilder(spark).build()
      .select($"trophy_collection_uuid", $"trophy_set_id")
      .persist()
    val trophies = postgresReader.read("psn.trophy").select(
      $"id" as "trophy_id",
      $"trophy_set_id",
      $"rank",
      $"title",
      $"detail",
      $"is_hidden",
      $"trophy_type",
      $"icon_url",
      $"game_group_id",
    )

    trophies
      .join(trophyCollections, "trophy_set_id")
      .withColumn("trophy_uuid", StringTransformUtils.toUuidUdf(concat_ws("_", $"trophy_collection_uuid", $"rank")))
      .select(
        $"trophy_id",
        $"trophy_uuid",
        $"trophy_collection_uuid",
        $"game_group_id",
        $"rank",
        $"title",
        $"detail",
        $"trophy_type",
        $"is_hidden",
        $"icon_url",
      )
  }

}
