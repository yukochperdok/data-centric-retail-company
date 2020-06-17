package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, StoreInterface}
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object StoreType {

  object ccTags {
    val idStoreTag = "idStore"
    val typeStoreTag = "typeStore"
    val creationDateTag = "creationDate"
    val createdByTag = "createdBy"
    val updatedByTag = "updatedBy"
    val updateDateTag = "updateDate"
  }

  object dbTags {
    val idStoreTag = "id_store"
    val typeStoreTag = "type_store"
    val creationDateTag = "creation_date"
    val createdByTag = "created_by"
    val updatedByTag = "updated_by"
    val updateDateTag = "update_date"
  }

  object implicits extends StoreTypeImplicits

  trait StoreTypeImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.typeStoreTag -> ccTags.typeStoreTag,
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

    implicit def readFromKuduTablesStoreType(implicit kuduCon: String): FromDbTable[StoreTypeView, Kudu] = {
      new FromKuduTable[StoreTypeView] {
        override def fromDB: DataFrame => Dataset[StoreTypeView] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idStoreTag,
            dbTags.typeStoreTag,
            Audit.dbTags.tsUpdateDlkTag
          ).transform(toCCAgg).as[StoreTypeView]
        }

        override val schemaName: String = "omnichannel_stream"
        override val tableName: String = "3b_location_type_oms"

        override def host: String = kuduCon
      }
    }

    implicit def writeToKuduTablesStoreType(implicit kuduCon: String): ToDbTable[StoreType, Kudu] = {
      new ToKuduTable[StoreType] {

        override def toDB: Dataset[StoreType] => DataFrame = {
          ds => {
            ds.toDF().transform(toDBAgg)
          }
        }

        override val schemaName: String = "omnichannel_stream"
        override val tableName: String = "3b_location_type_oms"

        override def host: String = kuduCon

      }
    }


  }

}

case class StoreTypeView(
  override val idStore: String,
  val typeStore: Boolean,
  val tsUpdateDlk: Timestamp
) extends StoreInterface

case class StoreType(
  override val idStore: String,
  val typeStore: Boolean,
  val creationDate: Timestamp,
  val createdBy: String,
  val updatedBy: String,
  val updateDate: Timestamp,
  val tsInsertDlk:Timestamp,
  val userInsertDlk: String,
  val tsUpdateDlk: Timestamp,
  val userUpdateDlk: String
) extends StoreInterface
