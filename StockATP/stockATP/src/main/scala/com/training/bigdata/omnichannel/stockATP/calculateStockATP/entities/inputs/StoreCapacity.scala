package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, StoreInterface}
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object StoreCapacity {

  object ccTags {
    val idStoreTag = "idStore"
    val idCapacityTag = "idCapacity"
    val creationDateTag = "creationDate"
    val createdByTag = "createdBy"
    val updatedByTag = "updatedBy"
    val updateDateTag = "updateDate"
  }

  object dbTags {
    val idStoreTag = "id_store"
    val idCapacityTag = "id_capacity"
    val creationDateTag = "creation_date"
    val createdByTag = "created_by"
    val updatedByTag = "updated_by"
    val updateDateTag = "update_date"
  }

  object implicits extends StoreCapacityImplicits

  trait StoreCapacityImplicits {

    private val db2cc: List[(String, String)] = List(
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.idCapacityTag -> ccTags.idCapacityTag,
      dbTags.creationDateTag -> ccTags.creationDateTag,
      dbTags.createdByTag -> ccTags.createdByTag,
      dbTags.updatedByTag -> ccTags.updatedByTag,
      dbTags.updateDateTag -> ccTags.updateDateTag,
      Audit.dbTags.tsInsertDlkTag -> Audit.ccTags.tsInsertDlkTag,
      Audit.dbTags.userInsertDlkTag -> Audit.ccTags.userInsertDlkTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag,
      Audit.dbTags.userUpdateDlkTag -> Audit.ccTags.userUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)
    private lazy val toDBAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._2, t._1) }).reduce(_ andThen _)

    implicit def readFromKuduTablesStoreCapacity(implicit kuduCon: String): FromDbTable[StoreCapacityView, Kudu] = {
      new FromKuduTable[StoreCapacityView] {
        override def fromDB: DataFrame => Dataset[StoreCapacityView] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idStoreTag,
            dbTags.idCapacityTag,
            Audit.dbTags.tsUpdateDlkTag
          ).transform(toCCAgg).as[StoreCapacityView]
        }

        override val schemaName: String = "omnichannel_stream"
        override val tableName: String = "3b_location_capacity_oms"

        override def host: String = kuduCon
      }
    }
    implicit def writeToKuduTablesStoreCapacity(implicit kuduCon: String): ToDbTable[StoreCapacity, Kudu] = {
      new ToKuduTable[StoreCapacity] {

        override def toDB: Dataset[StoreCapacity] => DataFrame = {
          ds => {
            ds.toDF().transform(toDBAgg)
          }
        }

        override val schemaName: String = "omnichannel_stream"
        override val tableName: String = "3b_location_capacity_oms"

        override def host: String = kuduCon

      }
    }

  }

}

case class StoreCapacityView(
  override val idStore: String,
  val idCapacity: Int,
  val tsUpdateDlk: Timestamp
) extends StoreInterface

case class StoreCapacity(
  override val idStore: String,
  val idCapacity: Int,
  val creationDate: Timestamp,
  val createdBy: String,
  val updatedBy: String,
  val updateDate: Timestamp,
  val tsInsertDlk:Timestamp,
  val userInsertDlk: String,
  val tsUpdateDlk: Timestamp,
  val userUpdateDlk: String
) extends StoreInterface
