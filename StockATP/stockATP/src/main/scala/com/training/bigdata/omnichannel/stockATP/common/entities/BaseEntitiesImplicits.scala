package com.training.bigdata.omnichannel.stockATP.common.entities

import com.training.bigdata.omnichannel.stockATP.common.util.AuditUtils
import org.apache.spark.sql.DataFrame

trait BaseEntitiesImplicits {
  def parseLastUpdate: DataFrame => DataFrame = df => df.transform(AuditUtils.parseUpdateInfo)
}
