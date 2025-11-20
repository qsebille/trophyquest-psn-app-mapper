package com.trophyquest

import com.trophyquest.config.JobArgs
import com.trophyquest.jobs.UserProfileJob
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object TrophyQuestMapperMain {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info(s"START PSN Mapper with args : ${args.mkString(" ")}")

    val spark = SparkSession.builder()
      .appName("TrophyQuest Transform")
      .master("local[*]")
      .getOrCreate()

    val jobArgs = new JobArgs(args)
    val jobName: String = jobArgs.jobName()

    logger.info(s"Using job name: $jobName")

    jobName match {
      case "user_profile" => new UserProfileJob(spark).run()
      case _ => throw new NotImplementedError(s"No job '$jobName' implemented")
    }

    spark.stop()
    logger.info("PSN Mapper job: SUCCESS")
  }

}
