package com.trophyquest.builders

import com.trophyquest.utils.{PostgresReader, StringTransformUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

class UserProfileBuilder(spark: SparkSession) {

  private final val postgresReader = new PostgresReader(spark)

  def build(): DataFrame = {
    import spark.implicits._

    postgresReader.read("psn.user_profile")
      .withColumn("user_uuid", StringTransformUtils.toUuidUdf($"id"))
      .select(
        $"id" as "user_psn_id",
        $"user_uuid",
        $"name",
        $"avatar_url",
      )
  }

}
