package org.mvrck.sphere.sandbox

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object Main {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("================================= start =================================")
    val spark = SparkSession.builder().getOrCreate()
    try {
      execute(spark)
    } finally {
      spark.stop()
    }
    logger.info("================================= end =================================")
  }

  def execute(spark: SparkSession): Unit = {
    logger.info("test")
  }

}
