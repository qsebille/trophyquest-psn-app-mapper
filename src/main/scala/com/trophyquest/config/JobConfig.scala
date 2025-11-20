package com.trophyquest.config

import java.util.Properties

object JobConfig {

  private val props = new Properties()

  props.load(
    Option(getClass.getClassLoader.getResourceAsStream("config.properties"))
      .getOrElse(throw new IllegalStateException("config.properties introuvable"))
  )

  private def get(key: String): String =
    Option(props.getProperty(key))
      .getOrElse(throw new IllegalStateException(s"Missing property : $key"))

  object postgres {
    val url: String = get("postgres.url")
    val user: String = get("postgres.user")
    val password: String = get("postgres.password")
  }

}