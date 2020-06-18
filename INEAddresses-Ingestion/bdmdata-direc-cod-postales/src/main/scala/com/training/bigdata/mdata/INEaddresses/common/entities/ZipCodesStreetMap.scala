package com.training.bigdata.mdata.INEaddresses.common.entities

import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.entities.StreetStretches._
import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.entities.{StreetStretches, StreetTypes}
import com.training.bigdata.mdata.INEaddresses.common.entities.interfaces.Audit
import com.training.bigdata.mdata.INEaddresses.common.util.Constants
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ZipCodesStreetMap {

  final val SPAIN = "ES"
  final val SPANISH = "S"
  final val WITHOUT_NUMBERING_TYPE = "SIN NÚMERO"
  final val ODD = "IMPAR"
  final val EVEN = "PAR"

  val streetStretchesFields =
    List(
      PREVIOUS_PROVINCE, PREVIOUS_LOCAL_COUNCIL_CODE, CUN, STREET_CODE, ZIP_CODE,
      NUMBERING_TYPE, INITIAL_NUMBER, INITIAL_LETTER, END_NUMBER, END_LETTER,
      STREET_NAME, TOWN_NAME, INE_MODIFICATION_DATE,
      Audit.tsInsertDlk, Audit.userInsertDlk, Audit.tsUpdateDlk, Audit.userUpdateDlk)

  val streetTypeFields =
    List(
      StreetTypes.STREET_TYPE, StreetTypes.STREET_TYPE_POSITION)

  val joinFieldsBetweenStreetStretchesAndTypes: Seq[String] = Seq(
    StreetTypes.PREVIOUS_PROVINCE, StreetTypes.PREVIOUS_LOCAL_COUNCIL_CODE, StreetTypes.STREET_CODE)

  val pkFields: Seq[String] = Seq(
    dbTags.lang, dbTags.country, dbTags.province, dbTags.municipality,
    CUN, STREET_CODE, ZIP_CODE, dbTags.numbering, INITIAL_NUMBER, INITIAL_LETTER, END_NUMBER, END_LETTER)

  val FILE_STRUCT_TYPE = StructType(Array(
    StructField( dbTags.lang,                   StringType, nullable = true),
    StructField( dbTags.country,                StringType, nullable = true),
    StructField( dbTags.province,               StringType, nullable = true),
    StructField( dbTags.municipality,           StringType, nullable = true),
    StructField( CUN,                           StringType, nullable = true),
    StructField( STREET_CODE,                  IntegerType, nullable = true),
    StructField( ZIP_CODE,                      StringType, nullable = true),
    StructField( dbTags.numbering,              StringType, nullable = true),
    StructField( INITIAL_NUMBER,               IntegerType, nullable = true),
    StructField( INITIAL_LETTER,                StringType, nullable = true),
    StructField( END_NUMBER,                   IntegerType, nullable = true),
    StructField( END_LETTER,                    StringType, nullable = true),
    StructField( dbTags.townCode,               StringType, nullable = true),
    StructField( STREET_NAME,                   StringType, nullable = true),
    StructField( TOWN_NAME,                     StringType, nullable = true),
    StructField( dbTags.ineModificationDate, TimestampType, nullable = true),
    StructField( Audit.tsInsertDlk,          TimestampType, nullable = true),
    StructField( Audit.userInsertDlk,           StringType, nullable = true),
    StructField( Audit.tsUpdateDlk,          TimestampType, nullable = true),
    StructField( Audit.userUpdateDlk,           StringType, nullable = true)
  ))


  object dbTags {
    val lang = "spras"
    val country = "land1"
    val province = "bland"
    val municipality = "local_council_code"
    val zipCode = ZIP_CODE
    val townCode = "town_code"
    val townName = TOWN_NAME
    val numbering = "numbering"
    val ineModificationDate = "ine_modification_date"
    val streetTypePosition = "is_prefix_else_suffix"

  }

  def addStreetTypes(streetTypesDF: DataFrame)(implicit spark: SparkSession): DataFrame => DataFrame = {

    df =>
      df
        .join(
          streetTypesDF
            .select(joinFieldsBetweenStreetStretchesAndTypes.head, joinFieldsBetweenStreetStretchesAndTypes.tail ++ streetTypeFields: _*),
          joinFieldsBetweenStreetStretchesAndTypes,
          Constants.LEFT)
        .withColumn(StreetTypes.STREET_TYPE_POSITION, lit(col(StreetTypes.STREET_TYPE_POSITION) === 0))

  }

  def toZipCodesStreetMap(implicit spark: SparkSession): DataFrame => DataFrame = {
    import org.apache.spark.sql.functions.{col, concat, substring, lit}

    df =>
      df
        .select(streetStretchesFields.head, streetStretchesFields.tail ++ streetTypeFields: _*)
        .withColumn(dbTags.lang, lit(SPANISH))
        .withColumn(dbTags.country, lit(SPAIN))
        .withColumnRenamed(PREVIOUS_LOCAL_COUNCIL_CODE, dbTags.municipality)
        .withColumnRenamed(PREVIOUS_PROVINCE, dbTags.province)
        .withColumn(dbTags.numbering,
          when(col(NUMBERING_TYPE) === lit(0), lit(WITHOUT_NUMBERING_TYPE))
            .when(col(NUMBERING_TYPE) === lit(1), lit(ODD))
            .when(col(NUMBERING_TYPE) === lit(2), lit(EVEN))
            .otherwise(lit(null))
        )
        .drop(NUMBERING_TYPE)
        .na.fill("", Seq(INITIAL_LETTER, END_LETTER))
        .withColumnRenamed(INE_MODIFICATION_DATE, dbTags.ineModificationDate)
        .withColumnRenamed(StreetTypes.STREET_TYPE_POSITION, dbTags.streetTypePosition)
        .withColumn(dbTags.townCode, concat(col(dbTags.province), col(dbTags.municipality), substring(col(CUN), 0, 5)))

  }

  def filterStreetName(implicit spark: SparkSession): DataFrame => DataFrame =
    df =>
      df.filter(
        row => !Option(row.getAs[String](STREET_NAME)).getOrElse("").isEmpty
      )

}

