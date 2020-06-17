package com.training.bigdata.omnichannel.stockATP.common.io.tables

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql._

sealed trait KuduHost

trait ToKuduTable[C] extends ToDbTable[C,Kudu]{
  def host:String


  def toDB:Dataset[C] => DataFrame = ds => ds.toDF()
  def toDBDF:DataFrame => DataFrame = df => df
  override def writeToDB(ds: Dataset[C], f: DataFrameWriter[Row] => DataFrameWriter[Row]): Unit = {
    implicit val kuduContext = new KuduContext(host, ds.sparkSession.sparkContext)
    val finalTableName = KuduUtils.getTableName(fullTableName)
    kuduContext.upsertRows(ds.transform(toDB),finalTableName)
  }
  override def writeToDBDF(df: DataFrame, f: DataFrameWriter[Row] => DataFrameWriter[Row]): Unit = {
    val kuduContext = new KuduContext(host, df.sparkSession.sparkContext)
    kuduContext.upsertRows(df.transform(toDBDF),fullTableName)
  }
}

trait FromKuduTable[C] extends FromDbTable[C, Kudu]{
  def host:String
  import org.apache.kudu.spark.kudu._
  override def read(implicit sparkSession: SparkSession): DataFrame = {
    implicit val kuduContext = new KuduContext(host, sparkSession.sparkContext)
    val finalTableName = KuduUtils.getTableName(fullTableName)
    sparkSession
      .read
      .options(Map("kudu.master" -> host, "kudu.table" -> finalTableName))
      .kudu
  }
}

trait FromKuduTableDF[C] extends FromDbTableDF[C, Kudu] {
  def host:String
  import org.apache.kudu.spark.kudu._
  override def readDF(implicit sparkSession: SparkSession): DataFrame = {
    implicit val kuduContext = new KuduContext(host, sparkSession.sparkContext)
    val finalTableName = KuduUtils.getTableName(fullTableName)
    sparkSession
      .read
      .options(Map("kudu.master" -> host, "kudu.table" -> finalTableName))
      .kudu
  }
}

trait FromToKuduTable[C] extends FromToDBTable[C,Kudu] with FromKuduTable[C] with ToKuduTable[C] with FromKuduTableDF[C]