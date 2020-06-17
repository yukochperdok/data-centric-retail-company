package com.training.bigdata.omnichannel.customerOrderReservation.utils

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import com.training.bigdata.omnichannel.customerOrderReservation.entities.Audit._

object AuditFieldsUtils {

  def addAuditFields(user: String, ts: Timestamp = new Timestamp(System.currentTimeMillis())): DataFrame => DataFrame = {
    import org.apache.spark.sql.functions._

    inputDF: DataFrame =>
      inputDF
        .withColumn(tsInsertDlk, lit(ts))
        .withColumn(userInsertDlk, lit(user))
        .withColumn(tsUpdateDlk, lit(ts))
        .withColumn(userUpdateDlk, lit(user))
  }
}
