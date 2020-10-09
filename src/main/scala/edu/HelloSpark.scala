package edu

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HelloSpark extends App with Serializable {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  if(args.length == 0) {
    logger.error("Usage HelloSpark filename")
    System.exit(1)
  }

  logger.info("Starting Spark Session")

  val sparConf = new SparkConf()
  sparConf.set("spark.app.name", "Hello Spark")
  sparConf.set("spark.master", "local[3]")

  val spark = SparkSession.builder()
    .config(sparConf)
    .getOrCreate()

  spark.stop()
}
