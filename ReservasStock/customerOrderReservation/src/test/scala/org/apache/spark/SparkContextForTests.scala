package org.apache.spark

import org.apache.spark.sql.SparkSession

trait SparkContextForTests {

  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("bdomnch-reservas-stock-atp")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.streaming.clock", "org.apache.spark.FixedClock")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

}
