package com.training.bigdata.mdata.INEaddresses.common.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkLocalTest {
  val spark: SparkSession = {

    def conf = {
      new SparkConf()
        .setMaster("local[1]")
        .setAppName("test-zipcodes")
        .set("spark.ui.enabled", "false")
        .set("spark.app.id", "testProcess")
        .set("spark.driver.host", "localhost")
    }

    val ss = SparkSession
      .builder()
      .config(conf)
      .master("local[1]")
      .appName("test-INEaddresses")
      .config("hive.exec.dynamic.partition.mode", "nonstrict" )
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.parquet.compression.codec", "snappy" )
      .config("parquet.compression", "snappy" )
      .enableHiveSupport()
      .getOrCreate()
    ss
  }
}

trait SparkLocalTest {
  implicit val spark: SparkSession = SparkLocalTest.spark
}
