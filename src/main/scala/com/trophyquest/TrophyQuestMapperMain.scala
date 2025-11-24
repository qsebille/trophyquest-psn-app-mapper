package com.trophyquest

import com.trophyquest.config.JobArgs
import com.trophyquest.jobs._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTimeUtils

import java.time.Duration

object TrophyQuestMapperMain {

  private final val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info(s"START PSN Mapper with args : ${args.mkString(" ")}")
    val startTime: Long = DateTimeUtils.currentTimeMillis()

    val spark = SparkSession.builder()
      .appName("TrophyQuest PSN Mapper")
      .master("local[*]")
      .getOrCreate()

    val jobArgs = new JobArgs(args)
    val jobName: String = jobArgs.jobName()

    logger.info(s"Using job name: $jobName")

    jobName match {
      case "game" => new GameJob(spark).run()
      case "trophy_collection" => new TrophyCollectionJob(spark).run()
      case "trophy" => new TrophyJob(spark).run()
      case "user_game" => new UserGameJob(spark).run()
      case "user_profile" => new UserProfileJob(spark).run()
      case "user_trophy_collection" => new UserTrophyCollectionJob(spark).run()
      case "user_trophy" => new UserTrophyJob(spark).run()
      case "all" =>
        new UserProfileJob(spark).run()
        new GameJob(spark).run()
        new UserGameJob(spark).run()
        new TrophyCollectionJob(spark).run()
        new UserTrophyCollectionJob(spark).run()
        new TrophyJob(spark).run()
        new UserTrophyJob(spark).run()
      case _ => throw new NotImplementedError(s"No job '$jobName' implemented")
    }

    spark.stop()

    val endTime: Long = DateTimeUtils.currentTimeMillis()
    val elapsedTime: Duration = Duration.ofMillis(endTime - startTime)
    logger.info(s"Job ended in ${elapsedTime.toSeconds} seconds")
    logger.info(s"PSN Mapper job $jobName: SUCCESS")
  }

}
