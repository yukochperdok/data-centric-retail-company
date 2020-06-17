package com.training.bigdata.omnichannel.stockATP.common.util

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, TempTags}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{Column, DataFrame}

object AuditUtils {

  def parseUpdateInfo: DataFrame => DataFrame = {
    import org.apache.spark.sql.functions._

    df =>
      df
        .withColumn(
          Audit.ccTags.tsUpdateDlkTag,
          when(
            col(Audit.ccTags.tsUpdateDlkTag) isNotNull,
            col(Audit.ccTags.tsUpdateDlkTag))
            .otherwise(col(Audit.ccTags.tsInsertDlkTag)))
        .drop(col(Audit.ccTags.tsInsertDlkTag))
  }

  def getLastUpdateDlkInInnerOrLeftJoin(rightTsUpdateDlkColumn: Column): DataFrame => DataFrame = {
    df =>
      df
        .withColumn(TempTags.tempTsUpdateDlkTag, rightTsUpdateDlkColumn)
        .drop(rightTsUpdateDlkColumn)
        .withColumn(
          Audit.ccTags.tsUpdateDlkTag,
          when(col(TempTags.tempTsUpdateDlkTag) > col(Audit.ccTags.tsUpdateDlkTag), col(TempTags.tempTsUpdateDlkTag))
            .otherwise(col(Audit.ccTags.tsUpdateDlkTag)))
        .drop(col(TempTags.tempTsUpdateDlkTag))
  }
}