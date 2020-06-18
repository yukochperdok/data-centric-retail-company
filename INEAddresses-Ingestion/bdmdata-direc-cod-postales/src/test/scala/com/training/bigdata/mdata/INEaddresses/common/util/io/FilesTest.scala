package com.training.bigdata.mdata.INEaddresses.common.util.io

import java.io.FileNotFoundException
import java.sql.Timestamp

import com.training.bigdata.mdata.INEaddresses.common.exception.{RowFormatException, WrongFileException}
import com.training.bigdata.mdata.INEaddresses.common.util.DatesUtils
import com.training.bigdata.mdata.INEaddresses.common.util.tag.TagTest.UnitTestTag
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

import scala.util.matching.Regex


class FilesTest extends FlatSpec with Matchers with DatasetSuiteBase {

  val FILENAME_PATTERN = "TRAM-NAL.F([0-9]{6})"

  val FILE_CONTENT_PATTERN: Regex = "^(.{2})([\\d]{3})([\\d]{8})$".r

  val FILE_TYPES_PARSER: Row => Row = row => {
    Row(
      row.getString(0),
      row.getString(1).toInt,
      DatesUtils.stringToTimestamp(row.getString(2), DatesUtils.yyyyMMdd_FORMATTER)
    )
  }

  val FILE_STRUCT_TYPE = StructType(Array(
  StructField(    "stringField",    StringType, nullable = true),
  StructField(   "integerField",   IntegerType, nullable = true),
  StructField( "timestampField", TimestampType, nullable = true)))

  "readTextFileFromHDFS" must "read all lines from a file in HDFS" taggedAs UnitTestTag in {
    val path = getClass.getClassLoader.getResource("read/correctFile").getPath

    val fileContent: RDD[String] = Files.readTextFileFromHDFS(path, "TRAM-NAL.F190630")(spark)

    fileContent.count shouldBe 1
    fileContent.take(1)(0) shouldBe "0100101001   00017010100100000            0124010001 9999    20190630 01001   0001701" +
      "                          ALEGRIADULANTZI          ALEGRIADULANTZI         01001TORRONDOA                00000" +
      "                                                              0124010001 9999"
  }

  it must "read all lines from a file in HDFS even when it contains special characters" taggedAs UnitTestTag in {
    val path = getClass.getClassLoader.getResource("read/withSpecialCharactersFile").getPath

    val fileContent: RDD[String] = Files.readTextFileFromHDFS(path, "TRAM-NAL.F190630")(spark)

    fileContent.count shouldBe 2
    fileContent.take(2)(0) shouldBe "0100101001   00017010100100000            0124010001 9999    20190630 01001   0001701" +
      "                        ALEGRIA-DULA'NTZI        ALEGRIA-DULA'NTZI         01001TORRONDOA                00000" +
      "                                                              0124010001 9999"
    fileContent.take(2)(1) shouldBe "0100101001   00017010100100000            0124010001 9999    20190630 01001   0001701" +
      "                        ALEGRIA-DULAÑNTZI        ALEGRIA-DULAÑNTZI         01001TORRONDOA                00000" +
      "                                                              0124010001 9999"
  }

  "getMostRecentFileOfPattern" must "read all lines from the newer file in HDFS (by name)" taggedAs UnitTestTag in {
    val path = getClass.getClassLoader.getResource("read/severalFiles").getPath

    val fileName: String = Files.getMostRecentFileOfPattern(path, FILENAME_PATTERN)(spark)

    fileName shouldBe "TRAM-NAL.F190630"
  }

  it must "throw an error when file does not match the pattern given" taggedAs UnitTestTag in {
    val path = getClass.getClassLoader.getResource("read/wrongNameFile").getPath

    val exceptionThrown = intercept[FileNotFoundException] {
      Files.getMostRecentFileOfPattern(path, FILENAME_PATTERN)(spark)
    }

    exceptionThrown.getMessage shouldBe s"No file that matches regex $FILENAME_PATTERN has been found in $path"
  }

  it must "throw an error when the folder does not exist" taggedAs UnitTestTag in {
    val path = getClass.getClassLoader.getResource("read").getPath + "/nonExistentPath"

    val exceptionThrown = intercept[FileNotFoundException] {
      Files.getMostRecentFileOfPattern(path, FILENAME_PATTERN)(spark)
    }

    exceptionThrown.getMessage shouldBe s"$path does not exist"
  }

  it must "throw an error when folder is empty" taggedAs UnitTestTag in {
    val path = getClass.getClassLoader.getResource("read/emptyFolder").getPath

    val exceptionThrown = intercept[FileNotFoundException] {
      Files.getMostRecentFileOfPattern(path, FILENAME_PATTERN)(spark)
    }

    exceptionThrown.getMessage shouldBe s"No file that matches regex $FILENAME_PATTERN has been found in $path"
  }

  "areFilesFromTheSameDate" must "return correctly if both files belong to the same date or not" taggedAs UnitTestTag in {
    val streetStretchesPattern = "TRAM-NAL.F([0-9]{6})"
    val streetTypesPattern = "VIAS-NAL.F([0-9]{6})"

    val streetStretchesFile190630 = "TRAM-NAL.F190630"
    val streetTypesFile190630 = "VIAS-NAL.F190630"
    val streetTypesFile180630 = "VIAS-NAL.F180630"

    val resultWhenIsEqual = Files.areFilesFromTheSameDate(
      streetStretchesFile190630, streetStretchesPattern, streetTypesFile190630, streetTypesPattern)

    val resultWhenIsNotEqual = Files.areFilesFromTheSameDate(
      streetStretchesFile190630, streetStretchesPattern, streetTypesFile180630, streetTypesPattern)

    resultWhenIsEqual shouldBe true
    resultWhenIsNotEqual shouldBe false
  }

  it must "return an exception when dates of different files cannot be parsed" taggedAs UnitTestTag in {
    val streetStretchesPattern = "TRAM-NAL.F([0-9]{6})"
    val streetTypesPattern = "VIAS-NAL.F([0-9]{6})"

    val streetStretchesFile190630 = "TRAM-NAL.F190630"
    val wrongFileName = "wrongFileName"

    intercept[WrongFileException] {
      Files.areFilesFromTheSameDate(
        streetStretchesFile190630, streetStretchesPattern, wrongFileName, streetTypesPattern)
    }.getMessage shouldBe "Correct INE files could not be found, so no street stretches, nor street types have been updated"
  }

  "parse" must "parse all rows given to street stretches dataframe" taggedAs UnitTestTag in {

    val fileContent: RDD[String] = spark.sparkContext.parallelize(Seq("0100120190630"))

    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("01", 1, new Timestamp(1561845600000L)))),
      FILE_STRUCT_TYPE
    )

    val result = Files.parse(
      FILE_CONTENT_PATTERN,
      FILE_TYPES_PARSER,
      FILE_STRUCT_TYPE
    )(spark)(fileContent)

    result.count shouldBe 1
    assertDataFrameEquals(expected, result)

  }

  it must "throw an error when file content does not match regex" taggedAs UnitTestTag in {

    val fileContentWithLessCharacters: RDD[String] = spark.sparkContext.parallelize(Seq("01001201906"))

    val fileContentWithMoreCharacters: RDD[String] = spark.sparkContext.parallelize(Seq("0100120190630000"))

    val fileContentWithErroneousTypes: RDD[String] = spark.sparkContext.parallelize(Seq("010b120190630"))

    val streetStretchesParser = Files.parse(
      FILE_CONTENT_PATTERN,
      FILE_TYPES_PARSER,
      FILE_STRUCT_TYPE
    )(spark)


    intercept[RowFormatException] {
      streetStretchesParser(fileContentWithLessCharacters).take(1)
    }.getMessage shouldBe "Could not parse correctly all file"

    intercept[RowFormatException] {
      streetStretchesParser(fileContentWithMoreCharacters).take(1)
    }.getMessage shouldBe "Could not parse correctly all file"

    intercept[RowFormatException] {
      streetStretchesParser(fileContentWithErroneousTypes).take(1)
    }.getMessage shouldBe "Could not parse correctly all file"

  }


}