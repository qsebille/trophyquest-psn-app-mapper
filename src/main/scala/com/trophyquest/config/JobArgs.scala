package com.trophyquest.config

import org.rogach.scallop._

class JobArgs(args: Seq[String]) extends ScallopConf(args) {

  val jobName: ScallopOption[String] = opt[String](
    name = "jobName",
    required = true,
    descr = "Required job identifier to run (usage: --jobName <name>)."
  )

  verify()
}
