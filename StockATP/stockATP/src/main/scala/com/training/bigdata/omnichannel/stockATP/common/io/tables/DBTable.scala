package com.training.bigdata.omnichannel.stockATP.common.io.tables

import org.apache.spark.sql._

import scala.annotation.implicitNotFound

sealed trait DBType
sealed trait Hive extends DBType
sealed trait Kudu extends DBType

trait DBTable[C, DB <: DBType] {
  val schemaName:String
  val tableName:String
  def fullTableName:String = schemaName+"."+tableName
}

object ToDbTable {
  object ops {
    implicit class ToDBTableOps[T](ds:Dataset[T]) {
      def writeTo[DB <: DBType](f: DataFrameWriter[Row] => DataFrameWriter[Row] = x => x)(implicit toDB:ToDbTable[T,DB]):Unit =toDB.writeToDB(ds,f)
    }
    implicit class ToDBTableOpsDF[T](df:DataFrame) {
      def writeTo[T, DB <: DBType](f: DataFrameWriter[Row] => DataFrameWriter[Row] = x => x)(implicit toDB:ToDbTable[T,DB]):Unit =toDB.writeToDBDF(df,f)
    }
  }
}

@implicitNotFound("Cannot find an ToDBTable implicit instance for ${C}")
trait ToDbTable[C, DB <: DBType] extends DBTable[C, DB] {
  def writeToDB(ds:Dataset[C], f: DataFrameWriter[Row] => DataFrameWriter[Row]):Unit
  def writeToDBDF(df:DataFrame, f: DataFrameWriter[Row] => DataFrameWriter[Row]):Unit
}

object FromDbTable {
  object ops {
    def read[T,DB <: DBType](implicit sparkSession: SparkSession, fromDB:FromDbTable[T, DB]):Dataset[T] = fromDB.readFromDb
    def readDF[T,DB <: DBType](implicit sparkSession: SparkSession, fromDB:FromDbTableDF[T, DB]):DataFrame = fromDB.readFromDbDF
  }
}

@implicitNotFound("Cannot find an FromDbTable implicit instance for ${C}")
trait FromDbTable[C, DB <: DBType] extends DBTable[C, DB] {
  def readFromDb(implicit sparkSession: SparkSession): Dataset[C] = read.transform(fromDB)
  def read(implicit sparkSession: SparkSession):DataFrame
  def fromDB:DataFrame => Dataset[C]
}

@implicitNotFound("Cannot find an FromDbTableDF implicit instance for ${C}")
trait FromDbTableDF[C, DB <: DBType] extends DBTable[C, DB] {
  def readFromDbDF(implicit sparkSession: SparkSession): DataFrame = readDF.transform(fromDBDF)
  def readDF(implicit sparkSession: SparkSession):DataFrame
  def fromDBDF:DataFrame => DataFrame
}

trait FromToDBTable[C, DB <: DBType] extends ToDbTable[C,DB] with FromDbTable[C,DB] with FromDbTableDF[C,DB]
