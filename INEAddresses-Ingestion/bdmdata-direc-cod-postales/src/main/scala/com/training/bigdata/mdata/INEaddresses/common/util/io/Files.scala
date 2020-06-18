package com.training.bigdata.mdata.INEaddresses.common.util.io

import java.io.FileNotFoundException

import com.training.bigdata.mdata.INEaddresses.common.util.log.FunctionalLogger
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import com.training.bigdata.mdata.INEaddresses.common.exception.{RowFormatException, WrongFileException}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat


object Files extends FunctionalLogger {

  private val dfs = { implicit spark: SparkSession => FileSystem.get(spark.sparkContext.hadoopConfiguration) }

  val UTF8     = "UTF-8"
  val ISO88591 = "ISO-8859-1"

  // character encoding of INE files is ISO-8859
  def readTextFileFromHDFS(path: String, fileName: String, charset: String = ISO88591)(implicit spark: SparkSession): RDD[String] = {
    if (charset == UTF8) {
      spark.sparkContext.textFile(s"$path/$fileName")
    } else {
      spark.sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](s"$path/$fileName").map(
        pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset)
      )
    }
  }

  def parse(contentPattern: Regex, contentTypesTransformation: Row => Row, schema: StructType)(implicit spark: SparkSession): RDD[String] => DataFrame =
    fileRDD => {
      fileRDD.cache

      val fileParsedAndTrimRDD =
        fileRDD
          .flatMap { line =>
            contentPattern
              .findFirstMatchIn(line)
              .map(field => field.subgroups) match {
                case Some(list) => Some(Row(list.map(_.trim): _*))
                case None =>
                  logNonFunctionalMessage(s"Could not parse line $line to regex $contentPattern")
                  None
              }
          }
          .map(contentTypesTransformation)

      if(fileParsedAndTrimRDD.count != fileRDD.count) throw new RowFormatException("Could not parse correctly all file")

      spark.createDataFrame(
        fileParsedAndTrimRDD,
        schema
      ).na.replace(schema.fieldNames, Map[String, String]("" -> null))

    }

  def getMostRecentFileOfPattern(path: String, pattern: String)(implicit spark: SparkSession) : String = {
    val filesInPath: Array[FileStatus] = Try {dfs(spark).listStatus(new Path(path))} match {
      case Success(fileList) => fileList
      case Failure(_) => throw new FileNotFoundException(s"$path does not exist")
    }

    Try {
      filesInPath
        .filter(fileStatus => fileStatus.isFile)
        .map(fileStatus => fileStatus.getPath.getName)
        .filter(fileName => fileName.matches(pattern))
        .sortWith(_ > _)
        .take(1)(0) // Should be just one
    } match {
      case Success(fileName) => fileName
      case Failure(_) =>
        throw new FileNotFoundException(s"No file that matches regex $pattern has been found in $path")
    }
  }

  def areFilesFromTheSameDate(fileName1: String, pattern1: String, fileName2: String, pattern2: String) : Boolean = {

    def getDateFromFileName(fileName: String, pattern: String) : String = {
        pattern.r
        .findFirstMatchIn(fileName)
        .map(field => field.subgroups) match {
          case Some(date) => date.head
          case None =>
            logNonFunctionalMessage(s"Could not get correct date from filename $fileName using $pattern")
            throw new WrongFileException(s"Correct INE files could not be found, so no street stretches, nor street types have been updated")
      }
    }

    getDateFromFileName(fileName1, pattern1) == getDateFromFileName(fileName2, pattern2)
  }

}
