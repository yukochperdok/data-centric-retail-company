package com.training.bigdata.omnichannel.stockATP.common.entities

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.Audit
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}


object ControlEjecucion {

  object ccTags {
    val idProcess = "idProcess"
    val lastExecutionDate = "lastExecutionDate"
    val initWriteDate = "initWriteDate"
    val finishWriteDate = "finishWriteDate"
  }

  object dbTags {
    val idProcess = "id_process"
    val lastExecutionDate = "last_execution_date"
    val initWriteDate = "init_write_date"
    val finishWriteDate = "finish_write_date"
  }

  object implicits extends ControlEjecucionImplicits

  trait ControlEjecucionImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idProcess -> ccTags.idProcess,
      dbTags.lastExecutionDate -> ccTags.lastExecutionDate,
      dbTags.initWriteDate -> ccTags.initWriteDate,
      dbTags.finishWriteDate -> ccTags.finishWriteDate,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag,
      Audit.dbTags.userUpdateDlkTag -> Audit.ccTags.userUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)
    private lazy val toDBAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._2, t._1) }).reduce(_ andThen _)

    implicit def readFromKuduTablesControlEjecucion(implicit kuduCon: String): FromDbTable[ControlEjecucionView, Kudu] = {
      new FromKuduTable[ControlEjecucionView] {
        override def fromDB: DataFrame => Dataset[ControlEjecucionView] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idProcess,
            dbTags.lastExecutionDate,
            dbTags.initWriteDate,
            dbTags.finishWriteDate
          ).transform(toCCAgg).as[ControlEjecucionView]
        }

        override val schemaName: String = "supply_user"
        override val tableName: String = "supply_control_ejecucion"

        override def host: String = kuduCon
      }
    }
    implicit def writeToKuduTablesControlEjecucion(implicit kuduCon: String): ToDbTable[ControlEjecucion, Kudu] = {
      new ToKuduTable[ControlEjecucion] {

        override def toDB: Dataset[ControlEjecucion] => DataFrame = {
          ds => {
            ds.toDF().transform(toDBAgg)
          }
        }

        override val schemaName: String = "supply_user"
        override val tableName: String = "supply_control_ejecucion"

        override def host: String = kuduCon

      }
    }

  }

}

case class ControlEjecucion(
  val idProcess: String,
  val lastExecutionDate: Timestamp,
  val initWriteDate: Timestamp,
  val finishWriteDate: Timestamp,
  val tsUpdateDlk: Timestamp,
  val userUpdateDlk: String)

case class ControlEjecucionView(
  val idProcess: String,
  val lastExecutionDate: Timestamp,
  val initWriteDate: Timestamp,
  val finishWriteDate: Timestamp)
