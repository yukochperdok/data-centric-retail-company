package com.holdenkarau.spark.testing

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

trait SparkContextProvider {
  def sc: SparkContext

  def appID: String = (this.getClass.getName
    + math.floor(math.random * 10E4).toLong.toString)

  def conf = {
    new SparkConf()
      .setMaster("local[1]")
      .setAppName("test-INEaddresses")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .set("spark.driver.host", "localhost")
  }


  /**
    * Setup work to be called when creating a new SparkContext. Default implementation
    * currently sets a checkpoint directory.
    *
    * This _should_ be called by the context provider automatically.
    */
  def setup(sc: SparkContext): Unit = {
    sc.setCheckpointDir(Utils.createTempDir().toPath().toString)
  }
}
