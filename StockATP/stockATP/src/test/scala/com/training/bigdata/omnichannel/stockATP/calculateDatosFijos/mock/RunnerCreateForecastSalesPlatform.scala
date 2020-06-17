package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.mock

import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._
import com.training.bigdata.omnichannel.stockATP.common.util._
import com.training.bigdata.omnichannel.stockATP.common.util.log.FunctionalLogger

import scala.util.{Failure, Success, Try}

object RunnerCreateForecastSalesPlatform extends App with SparkLocalTest with DefaultConf with FunctionalLogger {

  final val DATABASE_FORECAST_LAST = "forecast_last"
  final val TABLE_FORECAST_SALES_PLATFORM = "forecast_sales_platform"

  val (kuduHost: String, optDataDate: Option[String]) = args.toList match {
    case Nil => ("172.17.0.2:7051", None)
    case x :: xs => (x, xs.headOption)
  }

  logInfo(s"The given kuduHost is ${kuduHost}")

  implicit val appConfig =
    defaultConfig
      .setDatosFijosParameters(DatosFijosParameters(List("S", "")))
      .setCommonParameters(
        CommonParameters(
          kuduHost = kuduHost,
          timezone = "Europe/Paris",
          defaultUnidadMedida = "EA",
          defaultNst = 100,
          defaultNsp = 100,
          defaultRotation = "B",
          defaultRegularSales = 0
        )
      )

  def createForecastSalesPlatform: Unit = {
    import spark.sql

    def createForecastSalesPlatformTable = {
      // Re-create table
      sql(s"DROP TABLE IF EXISTS $DATABASE_FORECAST_LAST.$TABLE_FORECAST_SALES_PLATFORM")
      sql(
        s"""CREATE TABLE IF NOT EXISTS $DATABASE_FORECAST_LAST.$TABLE_FORECAST_SALES_PLATFORM
        (
             date_sale                 TIMESTAMP   COMMENT "fecha de venta",
             matnr                     STRING      COMMENT "codigo de articulo",
             werks                     STRING      COMMENT "codigo de tienda",
             regular_sales_forecast    DOUBLE      COMMENT "prevision de venta regular",
             unit_measure              STRING      COMMENT "unidad de medida",
             ts_insert_dlk             TIMESTAMP   COMMENT "timestamp de inserción",
             user_insert_dlk           STRING      COMMENT "usuario de inserción",
             ts_update_dlk             TIMESTAMP   COMMENT "timestamp de actualizacion",
             user_update_dlk           STRING      COMMENT "usuario de actualizacion"
        )
         COMMENT 'Sales forecast platform'
         STORED AS PARQUET """.stripMargin
      )
    }

    def insertInfo = {
      //Inserts

      sql(
        s"""Insert into $DATABASE_FORECAST_LAST.$TABLE_FORECAST_SALES_PLATFORM
          |values (
          |   "2018-07-29 00:00:00",
          |   "5",
          |   "P11",
          |   510,
          |   "1",
          |   "2018-07-29 00:00:00",
          |   "user",
          |   "2018-07-29 00:00:00",
          |   "user") """.stripMargin)

      sql(
        s"""Insert into $DATABASE_FORECAST_LAST.$TABLE_FORECAST_SALES_PLATFORM
           |values (
           |   "2018-07-30 00:00:00",
           |   "5",
           |   "P11",
           |   260,
           |   "1",
           |   "2018-07-29 00:00:00",
           |   "user",
           |   "2018-07-29 00:00:00",
           |   "user") """.stripMargin)

      sql(
        s"""Insert into $DATABASE_FORECAST_LAST.$TABLE_FORECAST_SALES_PLATFORM
           |values (
           |   "2018-07-30 00:00:00",
           |   "1",
           |   "11",
           |   260,
           |   "1",
           |   "2018-07-29 00:00:00",
           |   "user",
           |   "2018-07-29 00:00:00",
           |   "user") """.stripMargin)

      sql(
        s"""Insert into $DATABASE_FORECAST_LAST.$TABLE_FORECAST_SALES_PLATFORM
           |values (
           |   "2018-08-26 00:00:00",
           |   "1",
           |   "11",
           |   60,
           |   "1",
           |   "2018-08-26 00:00:00",
           |   "user",
           |   "2018-08-26 00:00:00",
           |   "user") """.stripMargin)

      sql(
        s"""Insert into $DATABASE_FORECAST_LAST.$TABLE_FORECAST_SALES_PLATFORM
           |values (
           |   "2018-09-23 00:00:00",
           |   "1",
           |   "11",
           |   40,
           |   "1",
           |   "2018-09-23 00:00:00",
           |   "user",
           |   "2018-09-23 00:00:00",
           |   "user") """.stripMargin)

      logInfo(s"Inserting data in $DATABASE_FORECAST_LAST")

    }

    // Create database
    Try {
      sql(s"CREATE DATABASE IF NOT EXISTS $DATABASE_FORECAST_LAST COMMENT 'forecast_last'")
    } match {
      case Success(_) =>
        logInfo(s"Creating schema $DATABASE_FORECAST_LAST")
        createForecastSalesPlatformTable
        insertInfo
      case Failure(f) =>
        logNonFunctionalMessage(s"${f.getClass}")
        logNonFunctionalMessage(s"Some critical problems to create  $DATABASE_FORECAST_LAST")
    }
  }


  createForecastSalesPlatform
}



