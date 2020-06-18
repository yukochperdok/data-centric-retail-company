package com.training.bigdata.mdata.INEaddresses.common.entities.interfaces

trait Debugger{
  def getDebugField(name: String): Option[String]
}

object Debugger{
  object ccTags {
  }
}

object Audit {
  val tsInsertDlk = "ts_insert_dlk"
  val tsUpdateDlk = "ts_update_dlk"
  val userInsertDlk = "user_insert_dlk"
  val userUpdateDlk = "user_update_dlk"
}

object PartitioningFields {
  final val YEAR = "year"
  final val MONTH = "month"
}

