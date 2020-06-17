package com.training.bigdata.omnichannel.stockATP.common.util

import org.apache.spark.sql.SparkSession

import java.io.File

object SparkLocalTest {
  val spark: SparkSession = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val ss = SparkSession
      .builder()
      .master("local[1]")
      .appName("test-stockATP")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields", 300)
      .enableHiveSupport()
      .getOrCreate()
    ss
  }
}

trait SparkLocalTest {
  implicit val spark: SparkSession = SparkLocalTest.spark
}
