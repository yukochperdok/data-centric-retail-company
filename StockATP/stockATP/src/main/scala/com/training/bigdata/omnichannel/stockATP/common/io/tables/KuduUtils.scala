package com.training.bigdata.omnichannel.stockATP.common.io.tables

import org.apache.kudu.spark.kudu.KuduContext

object KuduUtils extends KuduUtils

trait KuduUtils {

  final val IMPALA_PREFIX = "impala::"

  /**
    * Retrieves Kudu table name regardless of the names assigned by impala
    *
    * @param fullName    Schema + table name for Kudu table
    * @param kuduContext container for Kudu client connections.
    * @return full Kudu table name, regardless of the names assigned by impala
    */

  def getTableName(fullName : String)(implicit kuduContext: KuduContext): String = {
    val possibleNames = List(fullName, IMPALA_PREFIX + fullName)
    possibleNames
      .find(kuduContext.tableExists)
      .getOrElse(fullName)
  }
}