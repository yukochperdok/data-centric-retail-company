package com.training.bigdata.omnichannel.stockATP.common.io.tables

import org.apache.spark.sql._

trait ToHiveTable[C] extends ToDbTable[C,Hive]{

  val numPartitions:Int

  def toDB:Dataset[C] => DataFrame = ds => ds.toDF
  def toDBDF:DataFrame => DataFrame = df => df

  def write(dataFrame: DataFrameWriter[Row]): Unit = dataFrame.saveAsTable(fullTableName)
  def writeToDB(ds:Dataset[C], f: DataFrameWriter[Row] => DataFrameWriter[Row]):Unit = f.andThen(write)(ds.transform(toDB).write)

  override def writeToDBDF(df: DataFrame, f: DataFrameWriter[Row] => DataFrameWriter[Row]): Unit = {
    f({df.transform(toDBDF).repartition(numPartitions).write}).insertInto(fullTableName)
  }
}

trait FromHiveTable[C] extends FromDbTable[C, Hive]{
  override def read(implicit sparkSession: SparkSession): DataFrame = sparkSession.table(fullTableName)
}

trait FromHiveTableDF[C] extends FromDbTableDF[C, Hive]{
  override def readDF(implicit sparkSession: SparkSession): DataFrame = sparkSession.table(fullTableName)
}

trait FromToHiveTable[C] extends FromToDBTable[C,Hive] with FromHiveTable[C] with ToHiveTable[C]