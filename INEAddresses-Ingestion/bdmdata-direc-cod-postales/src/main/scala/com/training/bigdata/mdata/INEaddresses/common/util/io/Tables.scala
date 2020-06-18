package com.training.bigdata.mdata.INEaddresses.common.util.io

import com.training.bigdata.mdata.INEaddresses.common.util.log.FunctionalLogger
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object Tables extends FunctionalLogger {

  type Kudu
  type Hive

  object OperationType extends Enumeration {
    val Insert = Value("I")
    val InsertIgnoreRows = Value("R")
    val Upsert = Value("U")
    val Update = Value("P")
  }

  trait DataFrames[T] {
    def writeTo(df: DataFrame, table: String, host: String = "", operationMode: OperationType.Value = OperationType.Insert, numPartitions: Int = 1)(implicit sparkSession: SparkSession): Try[Boolean]
    def deleteFrom(df: DataFrame, table: String, host: String = "")(implicit sparkSession: SparkSession): Try[Boolean]
    def readFrom(table: String, host: String = "")(implicit sparkSession: SparkSession): Try[DataFrame]
  }

  object Implementations {

    implicit val KuduDs: DataFrames[Kudu] = new DataFrames[Kudu] {

      /** Here numPartitions is not used, as partitions are already defined in Kudu Table */
      override def writeTo(df: DataFrame, table: String, host: String, operationMode: OperationType.Value = OperationType.Insert, numPartitions: Int = 1)(implicit sparkSession: SparkSession): Try[Boolean] = {
        val kuduContext = new KuduContext(host, sparkSession.sparkContext)
        if (kuduContext.tableExists(table)) {
          operationMode match {
            case OperationType.Insert =>
              kuduContext.insertRows(df, table)
              Success(true)

            case OperationType.Upsert =>
              kuduContext.upsertRows(df, table)
              Success(true)

            case OperationType.Update =>
              kuduContext.updateRows(df, table)
              Success(true)

            case OperationType.InsertIgnoreRows =>
              kuduContext.insertIgnoreRows(df, table)
              Success(true)

            case _ => Failure(new RuntimeException(s"Not allowed operation"))
          }
        }
        else {
          Failure(new RuntimeException(s"Table $table does not exist in kudu"))
        }
      }

      override def deleteFrom(df: DataFrame, table: String, host: String)(implicit sparkSession: SparkSession): Try[Boolean] = {
        val kuduContext = new KuduContext(host, sparkSession.sparkContext)
        if (kuduContext.tableExists(table)) {
          kuduContext.deleteRows(df, table)
          Success(true)
        }
        else {
          Failure(new RuntimeException(s"Table $table does not exist in kudu"))
        }
      }

      override def readFrom(table: String, host: String)(implicit sparkSession: SparkSession): Try[DataFrame] = {
        import org.apache.kudu.spark.kudu._
        val kuduContext = new KuduContext(host, sparkSession.sparkContext)
        if (kuduContext.tableExists(table)) {
          Success(sparkSession
            .read
            .options(Map("kudu.master" -> host, "kudu.table" -> table))
            .kudu)
        }
        else {
          logAlert(s"The $table table is not available")
          Failure(new RuntimeException(s"Table $table does not exist in kudu"))
        }
      }
    }

    implicit val HiveDs: DataFrames[Hive] = new DataFrames[Hive] {

      /** Here numPartitions is used as number of files written in each Hive partition */
      override def writeTo(df: DataFrame, table: String, host: String = "", operationMode: OperationType.Value = OperationType.Insert, numPartitions: Int = 1 )(implicit sparkSession: SparkSession): Try[Boolean] = {
        operationMode match {
          case OperationType.Insert =>
            if (sparkSession.catalog.tableExists(table)) {
              df.repartition(numPartitions).write.mode(SaveMode.Overwrite).insertInto(table)
              Success(true)
            }
            else {
              Failure(new RuntimeException(s"Table $table does not exist in hive"))
            }

          case _ => Failure(new RuntimeException(s"Not allowed operation"))
        }

      }

      override def deleteFrom(df: DataFrame, table: String, host: String)(implicit sparkSession: SparkSession): Try[Boolean] = Failure(new RuntimeException(s"Not allowed operation"))

      override def readFrom(table: String, host: String = "")(implicit sparkSession: SparkSession): Try[DataFrame] = {
        if (sparkSession.catalog.tableExists(table))
          Success(sparkSession.table(table))
        else
          Failure(new RuntimeException(s"Table $table does not exist in hive"))
      }
    }
  }

  implicit class DataFramesWriteOperations(dataFrame: DataFrame)(implicit val sparkSession: SparkSession) {
    def writeTo[T](table: String, host: String = "", operationMode: OperationType.Value = OperationType.Insert, numPartitions: Int = 1)
      (implicit df: DataFrames[T]): Try[Boolean] =
      df.writeTo(dataFrame, table, host, operationMode, numPartitions)

    def deleteFrom[T](table: String, host: String = "")(implicit df: DataFrames[T]): Try[Boolean] = df.deleteFrom(dataFrame, table, host)
  }

  implicit class DataFramesReadOperations(table: String)(implicit val sparkSession: SparkSession) {
    def readFrom[T](host: String = "")(implicit df: DataFrames[T]): Try[DataFrame] =
      df.readFrom(table, host)
  }

}
