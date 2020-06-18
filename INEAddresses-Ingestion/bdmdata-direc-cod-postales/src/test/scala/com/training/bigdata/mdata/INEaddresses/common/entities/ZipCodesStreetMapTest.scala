package com.training.bigdata.mdata.INEaddresses.common.entities

import java.sql.Timestamp

import com.training.bigdata.mdata.INEaddresses.common.util.DatesUtils
import com.training.bigdata.mdata.INEaddresses.common.util.tag.TagTest.UnitTestTag
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers}
import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.entities.StreetStretches._
import com.training.bigdata.mdata.INEaddresses.common.entities.ZipCodesStreetMap.dbTags
import com.training.bigdata.mdata.INEaddresses.common.entities.interfaces.Audit
import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.entities.StreetTypes

class ZipCodesStreetMapTest extends FlatSpec with Matchers with DatasetSuiteBase {

  val FILE_STRUCT_TYPE_WITH_AUDIT = StructType(Array(
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
    StructField( END_LETTER,                   StringType, nullable = true),
    StructField( Audit.tsInsertDlk,         TimestampType, nullable = true),
    StructField( Audit.userInsertDlk,          StringType, nullable = true),
    StructField( Audit.tsUpdateDlk,         TimestampType, nullable = true),
    StructField( Audit.userUpdateDlk,          StringType, nullable = true)
  ))

  val ZIP_CODES_STREET_MAP_STRUCT_TYPE = StructType(Array(
    StructField( dbTags.lang,                    StringType, nullable = false),
    StructField( dbTags.country,                 StringType, nullable = false),
    StructField( dbTags.province,                StringType, nullable = true),
    StructField( dbTags.municipality,            StringType, nullable = true),
    StructField( CUN,                            StringType, nullable = true),
    StructField( STREET_CODE,                   IntegerType, nullable = true),
    StructField( ZIP_CODE,                       StringType, nullable = true),
    StructField( dbTags.numbering,               StringType, nullable = true),
    StructField( INITIAL_NUMBER,                IntegerType, nullable = true),
    StructField( INITIAL_LETTER,                 StringType, nullable = false),
    StructField( END_NUMBER,                    IntegerType, nullable = true),
    StructField( END_LETTER,                     StringType, nullable = false),
    StructField( dbTags.townCode,                StringType, nullable = true),
    StructField( STREET_NAME,                    StringType, nullable = true),
    StructField( TOWN_NAME,                      StringType, nullable = true),
    StructField( dbTags.ineModificationDate,  TimestampType, nullable = true),
    StructField( Audit.tsInsertDlk,           TimestampType, nullable = true),
    StructField( Audit.userInsertDlk,            StringType, nullable = true),
    StructField( Audit.tsUpdateDlk,           TimestampType, nullable = true),
    StructField( Audit.userUpdateDlk,            StringType, nullable = true),
    StructField( StreetTypes.STREET_TYPE,        StringType, nullable = true),
    StructField( dbTags.streetTypePosition,     BooleanType, nullable = true)
  ))

  val STREET_STRETCHES_AND_TYPES_JOINED_STRUCT_TYPE = StructType(Array(
    StructField( PREVIOUS_PROVINCE,                 StringType, nullable = true),
    StructField( PREVIOUS_LOCAL_COUNCIL_CODE,       StringType, nullable = true),
    StructField( PREVIOUS_DISTRICT,                 StringType, nullable = true),
    StructField( PREVIOUS_SECTION,                  StringType, nullable = true),
    StructField( PREVIOUS_SECTION_LETTER,           StringType, nullable = true),
    StructField( PREVIOUS_SUBSECTION,               StringType, nullable = true),
    StructField( PREVIOUS_CUN,                      StringType, nullable = true),
    StructField( PREVIOUS_STREET_CODE,             IntegerType, nullable = true),
    StructField( PREVIOUS_PSEUDOSTREET_CODE,       IntegerType, nullable = true),
    StructField( PREVIOUS_SQUARE,                   StringType, nullable = true),
    StructField( PREVIOUS_ZIP_CODE,                 StringType, nullable = true),
    StructField( PREVIOUS_NUMBERING_TYPE,           StringType, nullable = true),
    StructField( PREVIOUS_INITIAL_NUMBER,          IntegerType, nullable = true),
    StructField( PREVIOUS_INITIAL_LETTER,           StringType, nullable = true),
    StructField( PREVIOUS_END_NUMBER,              IntegerType, nullable = true),
    StructField( PREVIOUS_END_LETTER,               StringType, nullable = true),
    StructField( INFORMATION_TYPE,                  StringType, nullable = true),
    StructField( REFUND_CAUSE,                      StringType, nullable = true),
    StructField( INE_MODIFICATION_DATE,          TimestampType, nullable = true),
    StructField( INE_MODIFICATION_CODE,             StringType, nullable = true),
    StructField( DISTRICT,                          StringType, nullable = true),
    StructField( SECTION,                           StringType, nullable = true),
    StructField( SECTION_LETTER,                    StringType, nullable = true),
    StructField( SUBSECTION,                        StringType, nullable = true),
    StructField( CUN,                               StringType, nullable = true),
    StructField( GROUP_ENTITY_SHORT_NAME,           StringType, nullable = true),
    StructField( TOWN_NAME,                         StringType, nullable = true),
    StructField( LAST_GRANULARITY_SHORT_NAME,       StringType, nullable = true),
    StructField( STREET_CODE,                      IntegerType, nullable = true),
    StructField( STREET_NAME,                       StringType, nullable = true),
    StructField( PSEUDOSTREET_CODE,                IntegerType, nullable = true),
    StructField( PSEUDOSTREET_NAME,                 StringType, nullable = true),
    StructField( CADASTRAL_SQUARE,                  StringType, nullable = true),
    StructField( ZIP_CODE,                          StringType, nullable = true),
    StructField( NUMBERING_TYPE,                    StringType, nullable = true),
    StructField( INITIAL_NUMBER,                   IntegerType, nullable = true),
    StructField( INITIAL_LETTER,                    StringType, nullable = true),
    StructField( END_NUMBER,                       IntegerType, nullable = true),
    StructField( END_LETTER,                        StringType, nullable = true),
    StructField( Audit.tsInsertDlk,              TimestampType, nullable = true),
    StructField( Audit.userInsertDlk,               StringType, nullable = true),
    StructField( Audit.tsUpdateDlk,              TimestampType, nullable = true),
    StructField( Audit.userUpdateDlk,               StringType, nullable = true),
    StructField( StreetTypes.STREET_TYPE,           StringType, nullable = true),
    StructField( StreetTypes.STREET_TYPE_POSITION, BooleanType, nullable = true)
  ))


  "filterStreetName" must " filter the ones that do not have street name" taggedAs UnitTestTag in {

    val now = new Timestamp(System.currentTimeMillis())
    val inputZipCodesStreetMapDF: DataFrame = spark.createDataFrame(sc.parallelize(
      List[Row](
        Row("S", "ES", "01", "001","0001700", 2205,"28001", "1", 1, "", 3, "" ,"1",  "street_name", "1", now, now, "user", now, "user", "CALLE", false),
        Row("S", "ES", "01", "002","0001799", 2205,"28002", "1", 1, "", 3, "" ,"1", "street_name2", "1", now, now, "user", now, "user", "CALLE", false),
        Row("S", "ES", "01", "003","0001701", 2205,"28001", "1", 1, "", 3, "" ,"1",             "", "1", now, now, "user", now, "user", "CALLE", false), //empty street name
        Row("S", "ES", "01", "003","0001701", 2205,"28001", "1", 1, "", 3, "" ,"1",           null, "1", now, now, "user", now, "user", "CALLE", false) //null street name
      )
    ), ZIP_CODES_STREET_MAP_STRUCT_TYPE)

    val expectedDF = spark.createDataFrame(sc.parallelize(
      List[Row](
        Row("S", "ES", "01", "001","0001700", 2205,"28001", "1", 1, "", 3, "" ,"1", "street_name","1", now, now, "user",now, "user", "CALLE", false),
        Row("S", "ES", "01", "002","0001799", 2205,"28002", "1", 1, "", 3, "" ,"1", "street_name2","1", now, now, "user",now, "user", "CALLE", false)
      )
    ), ZIP_CODES_STREET_MAP_STRUCT_TYPE)

    val resultDF = inputZipCodesStreetMapDF.transform(ZipCodesStreetMap.filterStreetName(spark))
    assertDataFrameEquals(expectedDF, resultDF)

  }

  "toZipCodesStreetMap" must " map to a ZipCodesStreetMap" taggedAs UnitTestTag in {

    val now = new Timestamp(System.currentTimeMillis())
    val ts1 = DatesUtils.stringToTimestamp("2019-06-30 00:00:00", DatesUtils.yyyyMMdd_hhmmss_FORMATTER)

    val inputZipCodesStreetMapDF: DataFrame = spark.createDataFrame(sc.parallelize(
      List[Row](
        // Without numbering type
        Row("01","001","01","001",null,null,"0001701",1001,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1001,"TORRONDOA",0,null,null,"01240","0",1,null,9999,"A",
          now,"user_test",now,"user_test", "CALLE", true),
        // Odd numbering type
        Row("01","001","01","001",null,null,"0001701",1001,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1002,"TORRONDOA",0,null,null,"01240","1",1,null,9999,"A",
          now,"user_test",now,"user_test", "KALE", false),
        // Even numbering type
        Row("01","001","01","001",null,null,"0001701",1001,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1003,"TORRONDOA",0,null,null,"01240","2",1,null,9999,"A",
          now,"user_test",now,"user_test", null, null),
        // Non existent numbering type
        Row("01","001","01","001",null,null,"0001701",1001,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1004,"TORRONDOA",0,null,null,"01240","3",1,null,9999,"A",
          now,"user_test",now,"user_test", null, null),
        // Null numbering type
        Row("01","001","01","001",null,null,"0001701",1001,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1005,"TORRONDOA",0,null,null,"01240",null,1,null,9999,"A",
          now,"user_test",now,"user_test", null, null)
    )), STREET_STRETCHES_AND_TYPES_JOINED_STRUCT_TYPE)


    val expectedDF = spark.createDataFrame(sc.parallelize(
      List[Row](
        Row("S", "ES", "01", "001","0001701", 1001,"01240", "SIN NÚMERO",1, "", 9999, "A" ,"0100100017", "TORRONDOA","ALEGRIADULANTZI",
          ts1, now, "user_test",now, "user_test", "CALLE", true),
        Row("S", "ES", "01", "001","0001701", 1002,"01240", "IMPAR",1, "", 9999, "A" ,"0100100017", "TORRONDOA","ALEGRIADULANTZI",
          ts1, now, "user_test",now, "user_test", "KALE", false),
        Row("S", "ES", "01", "001","0001701", 1003,"01240", "PAR",1, "", 9999, "A" ,"0100100017", "TORRONDOA","ALEGRIADULANTZI",
          ts1, now, "user_test",now, "user_test", null, null),
        Row("S", "ES", "01", "001","0001701", 1004,"01240", null,1, "", 9999, "A" ,"0100100017", "TORRONDOA","ALEGRIADULANTZI",
          ts1, now, "user_test",now, "user_test", null, null),
        Row("S", "ES", "01", "001","0001701", 1005,"01240", null,1, "", 9999, "A" ,"0100100017", "TORRONDOA","ALEGRIADULANTZI",
          ts1, now, "user_test",now, "user_test", null, null)
      )
    ), ZIP_CODES_STREET_MAP_STRUCT_TYPE)

    val resultDF = inputZipCodesStreetMapDF.transform(
      ZipCodesStreetMap.toZipCodesStreetMap(spark)
    )
    .select(expectedDF.columns.head, expectedDF.columns.tail: _*)
    .orderBy(STREET_CODE)

    assertDataFrameEquals(expectedDF, resultDF)

  }

  "addStreetTypes" must " add street type and position to street stretches dataframe" taggedAs UnitTestTag in {

    val now = new Timestamp(System.currentTimeMillis())
    val ts1 = DatesUtils.stringToTimestamp("2019-06-30 00:00:00", DatesUtils.yyyyMMdd_hhmmss_FORMATTER)

    val inputZipCodesStreetMapDF: DataFrame = spark.createDataFrame(sc.parallelize(
      List[Row](
        Row("01","001","01","001",null,null,"0001701",1001,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1001,"TORRONDOA",0,null,null,"01240","1",1,null,9999,"A",
          now,"user_test",now,"user_test"),
        Row("01","001","01","001",null,null,"0001701",1002,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1002,"TORRONDOA",0,null,null,"01240","1",1,null,9999,"A",
          now,"user_test",now,"user_test"),
        Row("01","001","01","001",null,null,"0001701",1003,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1003,"TORRONDOA",0,null,null,"01240","1",1,null,9999,"A",
          now,"user_test",now,"user_test")
      )), FILE_STRUCT_TYPE_WITH_AUDIT)

    val inputStreetTypesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("01", "001", 1001, null, null, new Timestamp(1561845600000L), null, 1001,  "KALE", 0, "TORRONDOA", "TORRONDOA"),
        Row("01", "001", 1003, null, null, new Timestamp(1561845600000L), null, 1003, "CALLE", 1, "TORRONDOA", "TORRONDOA"))),
      StreetTypes.FILE_STRUCT_TYPE
    )

    val expectedDF = spark.createDataFrame(sc.parallelize(
      List[Row](
        Row("01","001","01","001",null,null,"0001701",1001,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1001,"TORRONDOA",0,null,null,"01240","1",1,null,9999,"A",
          now,"user_test",now,"user_test", "KALE", true),
        Row("01","001","01","001",null,null,"0001701",1002,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1002,"TORRONDOA",0,null,null,"01240","1",1,null,9999,"A",
          now,"user_test",now,"user_test", null, null),
        Row("01","001","01","001",null,null,"0001701",1003,0,null,"01240","1",1,null,9999,null,null,null,ts1,null,"01","001",
          null,null,"0001701",null,"ALEGRIADULANTZI","ALEGRIADULANTZI",1003,"TORRONDOA",0,null,null,"01240","1",1,null,9999,"A",
          now,"user_test",now,"user_test", "CALLE", false)
      )), STREET_STRETCHES_AND_TYPES_JOINED_STRUCT_TYPE)

    val resultDF = inputZipCodesStreetMapDF.transform(
      ZipCodesStreetMap.addStreetTypes(inputStreetTypesDF)(spark)
    )
    .select(expectedDF.columns.head, expectedDF.columns.tail: _*)
    .orderBy(STREET_CODE)

    assertDataFrameEquals(expectedDF, resultDF)

  }
}
