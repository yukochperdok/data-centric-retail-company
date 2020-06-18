package com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.entities

import com.training.bigdata.mdata.INEaddresses.common.util.DatesUtils
import com.training.bigdata.mdata.INEaddresses.common.util.io.Files
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.matching.Regex

object StreetTypes {

  /** This regex separates the subgroups in each file line.
    * This subgroups are the same as the fields in FILE_STRUCT_TYPE and in the same order.
    */
  val FILE_CONTENT_PATTERN: Regex = ("^([\\d]{2})([\\d]{3})([\\d]{5})" +
    "(.{1})(.{2})([\\d]{8})([\\s]{1})" +
    "([\\d]{5})(.{5})([\\d]{1})(.{50})(.{25})$").r

  final val PREVIOUS_PROVINCE = "previous_bland"
  final val PREVIOUS_LOCAL_COUNCIL_CODE = "previous_local_council_code"
  final val PREVIOUS_STREET_CODE = "previous_street_code"
  final val INFORMATION_TYPE = "information_type"
  final val REFUND_CAUSE = "refund_cause"
  final val INE_MODIFICATION_DATE = "INE_modification_date"
  final val INE_MODIFICATION_CODE = "INE_modification_code"
  final val STREET_CODE = "street_code"
  final val STREET_TYPE = "street_type"
  final val STREET_TYPE_POSITION = "street_type_position"
  final val STREET_NAME = "street_name"
  final val SHORT_STREET_NAME = "short_street_name"

  val FILE_STRUCT_TYPE = StructType(Array(
    StructField( PREVIOUS_PROVINCE,            StringType, nullable = true),
    StructField( PREVIOUS_LOCAL_COUNCIL_CODE,  StringType, nullable = true),
    StructField( PREVIOUS_STREET_CODE,        IntegerType, nullable = true),
    StructField( INFORMATION_TYPE,             StringType, nullable = true),
    StructField( REFUND_CAUSE,                 StringType, nullable = true),
    StructField( INE_MODIFICATION_DATE,     TimestampType, nullable = true),
    StructField( INE_MODIFICATION_CODE,        StringType, nullable = true),
    StructField( STREET_CODE,                 IntegerType, nullable = true),
    StructField( STREET_TYPE,                  StringType, nullable = true),
    StructField( STREET_TYPE_POSITION,        IntegerType, nullable = true),
    StructField( STREET_NAME,                  StringType, nullable = true),
    StructField( SHORT_STREET_NAME,            StringType, nullable = true)
  ))

  val FILE_TYPES_PARSER: Row => Row = row => {
    Row(
      row.getString(0),
      row.getString(1),
      row.getString(2).toInt,
      row.getString(3),
      row.getString(4),
      DatesUtils.stringToTimestamp(row.getString(5), DatesUtils.yyyyMMdd_FORMATTER),
      row.getString(6),
      row.getString(7).toInt,
      row.getString(8),
      row.getString(9).toInt,
      row.getString(10),
      row.getString(11)
    )
  }

  def parse(fileRDD: RDD[String])(implicit spark: SparkSession): DataFrame = Files.parse(
    StreetTypes.FILE_CONTENT_PATTERN,
    StreetTypes.FILE_TYPES_PARSER,
    StreetTypes.FILE_STRUCT_TYPE
  )(spark)(fileRDD)

}
