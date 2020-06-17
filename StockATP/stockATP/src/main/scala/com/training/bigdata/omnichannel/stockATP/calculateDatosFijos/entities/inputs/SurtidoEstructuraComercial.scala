package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.SubfamilyInterface
import com.training.bigdata.omnichannel.stockATP.common.io.tables.{FromDbTable, FromKuduTable, Kudu}
import org.apache.spark.sql.{DataFrame, Dataset}

object SurtidoEstructuraComercial {

  val SUBFAMILY_LEVEL: String = ""

  object ccTags {
    val idSubfamilyTag = "idSubfamily"
    val levelTag = "level"
    val sectorTag = "sector"
  }

  object dbTags {
    val idSubfamilyTag = "id_com_struct"
    val levelTag = "level"
    val sectorTag = "sector"
  }

  object implicits extends SurtidoEstructuraComercialImplicits

  trait SurtidoEstructuraComercialImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idSubfamilyTag -> ccTags.idSubfamilyTag,
      dbTags.levelTag -> ccTags.levelTag,
      dbTags.sectorTag -> ccTags.sectorTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesSurtidoEstructuraComercial(implicit kuduCon: String): FromDbTable[SurtidoEstructuraComercial, Kudu] = {
      new FromKuduTable[SurtidoEstructuraComercial] {
        override def fromDB: DataFrame => Dataset[SurtidoEstructuraComercial] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idSubfamilyTag,
            dbTags.levelTag,
            dbTags.sectorTag).transform(toCCAgg).as[SurtidoEstructuraComercial]
        }

        override val schemaName: String = "mdata_user"
        override val tableName: String = "commercial_structure"

        override def host: String = kuduCon
      }
    }

  }

}

case class SurtidoEstructuraComercial(
  idSubfamily: String,
  level: String,
  sector: String) extends SubfamilyInterface

case class SurtidoEstructuraComercialView(
  idSubfamily: String,
  sector: String) extends SubfamilyInterface


