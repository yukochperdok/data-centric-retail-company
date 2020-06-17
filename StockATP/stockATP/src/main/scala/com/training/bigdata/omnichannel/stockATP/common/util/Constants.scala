package com.training.bigdata.omnichannel.stockATP.common.util

import java.sql.Timestamp
import java.time.format.DateTimeFormatter

case class ArticleTypeAndCategory(articleType: String, category: String)

object Constants {

  final val ONLY_DIGITS_DATE_REGEX = "(\\d{8})"
  final val DEFAULT_TIMEZONE: String = "Europe/Paris"
  final val DEFAULT_DATETIME_FORMAT: String = "yyyy-MM-dd HH:mm:ss"
  final val DEFAULT_DATETIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_DATETIME_FORMAT)
  final val DEFAULT_DAY_FORMAT: String = "yyyyMMdd"
  final val IN_PROCESS_ORDERS_DATETIME_FORMAT = "yyyyMMdd HH:mm:ss"
  final val IN_PROCESS_ORDERS_DATETIME_FORMATTER: DateTimeFormatter =
    DateTimeFormatter.ofPattern(IN_PROCESS_ORDERS_DATETIME_FORMAT)
  final val HYPHEN_SEPARATOR = "-"
  final val NO_SEPARATOR = ""
  final val DEFAULT_ORDER_TIMESTAMP = new Timestamp(0L)
  final val INNER = "inner"
  final val OUTER = "outer"
  final val LEFT = "left"
  final val MAX_PROJECT_DAYS = 100
  final val ATP_PLANNING_DAYS = MAX_PROJECT_DAYS + 1
  final val SALES_FORECAST_DAYS = ATP_PLANNING_DAYS + 1

  final val PURCHASE_ARTICLE_TYPE_AND_CATEGORY_WITH_UNIQUE_SALE_ARTICLE: List[ArticleTypeAndCategory] =
    ArticleTypeAndCategory("ZVLG", "12") ::
    Nil

  val BOX_AND_PREPACK_TYPE_AND_CATEGORY: List[ArticleTypeAndCategory] =
    ArticleTypeAndCategory("ZTEX","11") ::
    ArticleTypeAndCategory("ZBOX","12") ::
    Nil

  val DEFAULT_FLAGS_ANULADO_BORRADO: List[String] = List("X","L")
  val DEFAULT_FLAG_DEVOLUCION: String = "X"
  val DEFAULT_FLAG_ENTREGADO: String = "X"

  val DEFAULT_LIST_TYPE_ORDERS: List[String] =
    List(
      "ZACT",
      "ZADT",
      "ZGPT",
      "ZCCP",
      "ZAXP",
      "ZAC1",
      "ZADE",
      "ZASP",
      "ZATP"
    )

  final val TIPO_ROTACION_AA = "A+".toLowerCase
  final val TIPO_ROTACION_A = "A".toLowerCase
  final val TIPO_ROTACION_B = "B".toLowerCase
  final val TIPO_ROTACION_C = "C".toLowerCase
  final val FRESH_PRODUCTS_SECTOR = "02"

  final val DAYS_TO_BREAK_PLANING = 5

  final val UNITY_MEASURE_TO_ROUND_DOWN = "EA"

  final val DEFAULT_SALES_MOVEMENT_CODE = "601"
}
