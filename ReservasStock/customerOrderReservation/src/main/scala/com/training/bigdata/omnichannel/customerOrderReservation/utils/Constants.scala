package com.training.bigdata.omnichannel.customerOrderReservation.utils

import java.time.format.DateTimeFormatter

object Constants {

  object Errors {
    final val RESERVATION_WITH_NULL_ARTICLE = "Customer order reservation with article null: "
    final val RESERVATION_WITH_NULL_STORE = "Customer order reservation with store null: "
    final val RESERVATION_WITH_NULL_PICKING_DATE = "Customer order reservation with picking date null: "
    final val WRONG_FORMAT_DATE = "Wrong format date: "
    final val ARTICLE_NOT_IN_TRANSCOD = "Article not found when transcoding: "
    final val STORE_NOT_IN_TRANSCOD = "Store not found when transcoding: "
  }

  object Sql {
    final val WHERE = "WHERE"
    final val LEFT = "left"
    final val INNER = "inner"
  }

  final val IMPALA_PREFIX = "impala::"
  final val KUDU_DELETE_NON_EXISTENT_PK_ERROR = "Not found: key not found"

  object Date {
    final val DEFAULT_TIMEZONE: String = "Europe/Madrid"
    final val DEFAULT_DATETIME_FORMAT: String = "yyyy-MM-dd HH:mm:ss.SSSXXX"
    final val DEFAULT_DATETIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_DATETIME_FORMAT)
  }

}
