package com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.entities

import com.training.bigdata.mdata.INEaddresses.common.util.DatesUtils
import com.training.bigdata.mdata.INEaddresses.common.util.io.Files
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.matching.Regex

object StreetStretches {

  /** This regex separates the subgroups in each file line.
    * This subgroups are the same as the fields in FILE_STRUCT_TYPE and in the same order.
    */
  val FILE_CONTENT_PATTERN: Regex = ("^([\\d]{2})([\\d]{3})(.{2})(.{3})(.{1})(.{2})([\\d]{7})([\\d]{5})" +
    "([\\d]{5})(.{12})(.{5})([\\d]{1})([\\d]{4})(.{1})([\\d]{4})(.{1})(.{1})(.{2})([\\d]{8})([\\s]{1})(.{2})" +
    "(.{3})(.{1})(.{2})([\\d]{7})(.{25})(.{25})(.{25})([\\d]{5})(.{25})([\\d]{5})(.{50})(.{12})([\\d]{5})([\\d]{1})" +
    "([\\d]{4})(.{1})([\\d]{4})(.{1})$").r

  final val PREVIOUS_PROVINCE = "previous_bland"
  final val PREVIOUS_LOCAL_COUNCIL_CODE = "previous_local_council_code"
  final val PREVIOUS_DISTRICT = "previous_district"
  final val PREVIOUS_SECTION = "previous_section"
  final val PREVIOUS_SECTION_LETTER = "previous_section_letter"
  final val PREVIOUS_SUBSECTION = "previous_subsection"
  final val PREVIOUS_CUN = "previous_cun"
  final val PREVIOUS_STREET_CODE = "previous_street_code"
  final val PREVIOUS_PSEUDOSTREET_CODE = "previous_pseudostreet_code"
  final val PREVIOUS_SQUARE = "previous_square"
  final val PREVIOUS_ZIP_CODE = "previous_zip_code"
  final val PREVIOUS_NUMBERING_TYPE = "previous_numbering_type"
  final val PREVIOUS_INITIAL_NUMBER = "previous_initial_number"
  final val PREVIOUS_INITIAL_LETTER = "previous_initial_letter"
  final val PREVIOUS_END_NUMBER = "previous_end_number"
  final val PREVIOUS_END_LETTER = "previous_end_letter"
  final val INFORMATION_TYPE = "information_type"
  final val REFUND_CAUSE = "refund_cause"
  final val INE_MODIFICATION_DATE = "INE_modification_date"
  final val INE_MODIFICATION_CODE = "INE_modification_code"
  final val DISTRICT = "district"
  final val SECTION = "section"
  final val SECTION_LETTER = "section_letter"
  final val SUBSECTION = "subsection"
  final val CUN = "cun"
  final val GROUP_ENTITY_SHORT_NAME = "group_entity_short_name"
  final val TOWN_NAME = "town_name"
  final val LAST_GRANULARITY_SHORT_NAME = "last_granularity_short_name"
  final val STREET_CODE = "street_code"
  final val STREET_NAME = "street_name"
  final val PSEUDOSTREET_CODE = "pseudostreet_code"
  final val PSEUDOSTREET_NAME = "pseudostreet_name"
  final val CADASTRAL_SQUARE = "cadastral_square"
  final val ZIP_CODE = "zip_code"
  final val NUMBERING_TYPE = "numbering_type"
  final val INITIAL_NUMBER = "initial_number"
  final val INITIAL_LETTER = "initial_letter"
  final val END_NUMBER = "end_number"
  final val END_LETTER = "end_letter"


  val FILE_STRUCT_TYPE = StructType(Array(
    StructField( PREVIOUS_PROVINCE,            StringType, nullable = true),
    StructField( PREVIOUS_LOCAL_COUNCIL_CODE,  StringType, nullable = true),
    StructField( PREVIOUS_DISTRICT,            StringType, nullable = true),
    StructField( PREVIOUS_SECTION,             StringType, nullable = true),
    StructField( PREVIOUS_SECTION_LETTER,      StringType, nullable = true),
    StructField( PREVIOUS_SUBSECTION,          StringType, nullable = true),
    StructField( PREVIOUS_CUN,                 StringType, nullable = true),
    StructField( PREVIOUS_STREET_CODE,        IntegerType, nullable = true),
    StructField( PREVIOUS_PSEUDOSTREET_CODE,  IntegerType, nullable = true),
    StructField( PREVIOUS_SQUARE,              StringType, nullable = true),
    StructField( PREVIOUS_ZIP_CODE,            StringType, nullable = true),
    StructField( PREVIOUS_NUMBERING_TYPE,      StringType, nullable = true),
    StructField( PREVIOUS_INITIAL_NUMBER,     IntegerType, nullable = true),
    StructField( PREVIOUS_INITIAL_LETTER,      StringType, nullable = true),
    StructField( PREVIOUS_END_NUMBER,         IntegerType, nullable = true),
    StructField( PREVIOUS_END_LETTER,          StringType, nullable = true),
    StructField( INFORMATION_TYPE,             StringType, nullable = true),
    StructField( REFUND_CAUSE,                 StringType, nullable = true),
    StructField( INE_MODIFICATION_DATE,     TimestampType, nullable = true),
    StructField( INE_MODIFICATION_CODE,        StringType, nullable = true),
    StructField( DISTRICT,                     StringType, nullable = true),
    StructField( SECTION,                      StringType, nullable = true),
    StructField( SECTION_LETTER,               StringType, nullable = true),
    StructField( SUBSECTION,                   StringType, nullable = true),
    StructField( CUN,                          StringType, nullable = true),
    StructField( GROUP_ENTITY_SHORT_NAME,      StringType, nullable = true),
    StructField( TOWN_NAME,                    StringType, nullable = true),
    StructField( LAST_GRANULARITY_SHORT_NAME,  StringType, nullable = true),
    StructField( STREET_CODE,                 IntegerType, nullable = true),
    StructField( STREET_NAME,                  StringType, nullable = true),
    StructField( PSEUDOSTREET_CODE,           IntegerType, nullable = true),
    StructField( PSEUDOSTREET_NAME,            StringType, nullable = true),
    StructField( CADASTRAL_SQUARE,             StringType, nullable = true),
    StructField( ZIP_CODE,                     StringType, nullable = true),
    StructField( NUMBERING_TYPE,               StringType, nullable = true),
    StructField( INITIAL_NUMBER,              IntegerType, nullable = true),
    StructField( INITIAL_LETTER,               StringType, nullable = true),
    StructField( END_NUMBER,                  IntegerType, nullable = true),
    StructField( END_LETTER,                   StringType, nullable = true)
  ))

  val FILE_TYPES_PARSER: Row => Row = row => {
    Row(
      row.getString(0),
      row.getString(1),
      row.getString(2),
      row.getString(3),
      row.getString(4),
      row.getString(5),
      row.getString(6),
      row.getString(7).toInt,
      row.getString(8).toInt,
      row.getString(9),
      row.getString(10),
      row.getString(11),
      row.getString(12).toInt,
      row.getString(13),
      row.getString(14).toInt,
      row.getString(15),
      row.getString(16),
      row.getString(17),
      DatesUtils.stringToTimestamp(row.getString(18), DatesUtils.yyyyMMdd_FORMATTER),
      row.getString(19),
      row.getString(20),
      row.getString(21),
      row.getString(22),
      row.getString(23),
      row.getString(24),
      row.getString(25),
      row.getString(26),
      row.getString(27),
      row.getString(28).toInt,
      row.getString(29),
      row.getString(30).toInt,
      row.getString(31),
      row.getString(32),
      row.getString(33),
      row.getString(34),
      row.getString(35).toInt,
      row.getString(36),
      row.getString(37).toInt,
      row.getString(38)
    )
  }

  def parse(fileRDD: RDD[String])(implicit spark: SparkSession): DataFrame = Files.parse(
    StreetStretches.FILE_CONTENT_PATTERN,
    StreetStretches.FILE_TYPES_PARSER,
    StreetStretches.FILE_STRUCT_TYPE
  )(spark)(fileRDD)

}
