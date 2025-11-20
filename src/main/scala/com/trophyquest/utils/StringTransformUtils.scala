package com.trophyquest.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.nio.charset.StandardCharsets
import java.text.Normalizer
import java.util.UUID

object StringTransformUtils {

  val toUuidUdf: UserDefinedFunction = udf((input: String) => toUuid(input))
  val toSlugUdf: UserDefinedFunction = udf((input: String) => toSlug(input))

  private def toSlug(input: String): String = {
    Normalizer
      .normalize(input, Normalizer.Form.NFD)
      .replaceAll("\\p{M}", "")
      .replaceAll("[^A-Za-z0-9\\s]", "")
      .trim
      .replaceAll("\\s+", "_")
      .toLowerCase
  }

  private def toUuid(input: String): String = {
    UUID.nameUUIDFromBytes(input.getBytes(StandardCharsets.UTF_8)).toString
  }

}
