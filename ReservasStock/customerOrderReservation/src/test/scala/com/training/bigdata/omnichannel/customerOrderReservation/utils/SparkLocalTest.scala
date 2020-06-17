package com.training.bigdata.omnichannel.customerOrderReservation.utils

import java.io.File

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkLocalTest {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("test-reservas")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .getOrCreate()

  val sparkContext: SparkContext = sparkSession.sparkContext
  val streamingContext: StreamingContext = new StreamingContext(sparkContext, Seconds(10))

}

trait SparkLocalTest {
  implicit val sparkSession: SparkSession = SparkLocalTest.sparkSession
  implicit val sparkContext: SparkContext = SparkLocalTest.sparkContext
  implicit val streamingContext: StreamingContext = SparkLocalTest.streamingContext

  val assertDataFrameEquals: (DataFrame, DataFrame) => Unit = {
    (dfExpected: DataFrame, dfFinal: DataFrame) => {
      (new JavaDatasetSuiteBase).assertDataFrameEquals(dfExpected, dfFinal)
    }
  }
}
