package com.trophyquest.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.nio.charset.StandardCharsets
import java.util.UUID

object Uuid {

  val getFromStringUDF: UserDefinedFunction = udf((input: String) => UUID.nameUUIDFromBytes(input.getBytes(StandardCharsets.UTF_8)).toString)

}
