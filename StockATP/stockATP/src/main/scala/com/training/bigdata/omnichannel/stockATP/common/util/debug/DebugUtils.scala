package com.training.bigdata.omnichannel.stockATP.common.util.debug

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.Debugger
import com.training.bigdata.omnichannel.stockATP.common.util.DebugParameters
import com.training.bigdata.omnichannel.stockATP.common.util.SparkPlanning.sparkPlanningOps
import com.training.bigdata.omnichannel.stockATP.common.util.log.FunctionalLogger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Try


object DebugUtils extends FunctionalLogger {

  implicit class ops(ds: Dataset[_]) {
    def logDSDebug(nameOfDs: String = "ds")(implicit debugOptions: DebugParameters): Unit = DebugUtils.debug(ds, nameOfDs)

    def logDSDebugExplain(nameOfDs: String = "ds")(implicit debugOptions: DebugParameters): Unit = DebugUtils.debugExplain(ds, nameOfDs)
  }

  def logDebugTransform[T](nameOfDs: String = "ds")(implicit debugOptions: DebugParameters): Dataset[T] => Dataset[T] =
    ds => {
      DebugUtils.debug(ds, nameOfDs)
      ds
    }

  def logDebugAndBreakPlanningTransform(nameOfDs: String = "df")(implicit debugOptions: DebugParameters, spark: SparkSession): DataFrame => DataFrame =
    df => {
      if (buildLogger.isDebugEnabled && (debugOptions.activateShowDataset || debugOptions.activateDebugDataset)) {
        val dfCopy = df.breakPlanning
        DebugUtils.debug(dfCopy, nameOfDs)
        dfCopy
      } else {
        df
      }
    }

  private def debug(ds: Dataset[_], nameOfDs: String = "ds")(implicit debugOptions: DebugParameters): Unit = {
    import org.apache.spark.sql.execution.debug._
    import org.apache.spark.sql.functions.{col, lit}

    if (buildLogger.isDebugEnabled) {
      if (debugOptions.activateShowDataset || debugOptions.activateDebugDataset) {
        logDebug(s"Dataset: [$nameOfDs]")
        if (debugOptions.activateShowDataset) {
          val optFirst = ds.cache.head(1).headOption
          optFirst.map {
            first =>
              debugOptions.filters.foldLeft(ds) {
                case (dsAccum, (key, value)) =>
                  Try {
                    first match {
                      case debugField: Debugger =>
                        logDebug(s"Dataset[Debugger] con key: [$key]")
                        debugField.getDebugField(key).fold(dsAccum) {
                          filterField => dsAccum.filter(col(filterField) === lit(value))
                        }

                      case _ =>
                        logDebug(s"Dataset[_] con key: [$key]")
                        dsAccum.filter(col(key) === lit(value))
                    }
                  }.getOrElse(dsAccum)
              }
          }.getOrElse(ds).show
        }

        if (debugOptions.activateDebugDataset) Try {
          ds.debug
        }.getOrElse(logDebug("Not supported debug"))
      }
    }
  }

  private def debugExplain(ds: Dataset[_], nameOfDs: String = "ds")(implicit debugOptions: DebugParameters): Unit = {
    if (buildLogger.isDebugEnabled) {
      if (debugOptions.activateExplain) {
        logDebug(s"Explain - Dataset: [$nameOfDs]")
        Try {
          ds.explain
        }.getOrElse(logDebug("Not supported explain"))
      }
    }
  }
}