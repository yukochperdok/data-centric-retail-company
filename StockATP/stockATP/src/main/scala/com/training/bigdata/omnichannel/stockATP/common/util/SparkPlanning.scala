package com.training.bigdata.omnichannel.stockATP.common.util

import org.apache.spark.sql.DataFrame

object SparkPlanning {

  implicit class sparkPlanningOps(dataFrame: DataFrame) {

    /**
      * From the given DataFrame creates a new identical one. This way spark planning
      * breaks and is not so lo long anymore, avoiding complex plannings which
      * take too much time and most of the times end up with infinite loops.
      *
      * @param dataFrame Dataframe to be recreated.
      * @return new DataFrame identical to the one given.
      */

    def breakPlanning: DataFrame = {
      val sparks = dataFrame.sparkSession
      sparks.createDataFrame(dataFrame.rdd, dataFrame.schema)
    }
  }

}
