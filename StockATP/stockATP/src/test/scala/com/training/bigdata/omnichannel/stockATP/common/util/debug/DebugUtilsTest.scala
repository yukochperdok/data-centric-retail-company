package com.training.bigdata.omnichannel.stockATP.common.util.debug

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._
import com.training.bigdata.omnichannel.stockATP.common.util.debug.DebugUtils.{ops, _}
import com.training.bigdata.omnichannel.stockATP.common.util.log.FunctionalLogger
import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest._
import com.training.bigdata.omnichannel.stockATP.common.util.{DebugParameters, DefaultConf}
import com.github.dwickern.macros.NameOf._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}

object FieldNames{
  val nameField1 = "field1"
  val nameField2 = "field2"
  val nameField3 = "field3"
  val nameField4 = "field4"
  val nameField5 = "field5"
  val nameField6 = "field6"
  val nameField7 = "field7"
  val nameField8 = "field8"
  val nameWrongField = "wrong_field"
}

case class DebugTest(
  field1: String,
  field2: Int,
  field3: Double,
  field4: Timestamp)

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.Debugger
case class DebugWithDebuggerTest(
  field1: String,
  field2: Int,
  field3: Double,
  field4: Timestamp) extends Debugger {
  override def getDebugField(name: String): Option[String] = name match {
    case FieldNames.nameField5 => Some(FieldNames.nameField1)
    case FieldNames.nameField6 => Some(FieldNames.nameField2)
    case FieldNames.nameField7 => Some(FieldNames.nameField3)
    case FieldNames.nameField8 => Some(FieldNames.nameField4)
    case _ => None
  }
}

class DebugUtilsTest extends FlatSpec with Matchers with DatasetSuiteBase with FunctionalLogger{

  import spark.implicits._
  override protected implicit def enableHiveSupport: Boolean = false


  trait DebugConfig extends DefaultConf {
    val debugConfig = defaultConfig.
      setDebugStockATPParameters(
        DebugParameters(true, true, true))

    val debugConfigWithFilters = defaultConfig.
      setDebugStockATPParameters(
        DebugParameters(true, true, true,
          Map(FieldNames.nameField1 -> "1")
        ))

    val debugConfigWithMoreFilters = defaultConfig.
      setDebugStockATPParameters(
        DebugParameters(true, true, true,
          Map(
            FieldNames.nameField7 -> "1.0"
          )
        ))

    val debugConfigWithEmptyFilters = defaultConfig.
      setDebugStockATPParameters(
        DebugParameters(true, true, true,
          Map.empty
        ))

    val debugConfigWithWrongFilters = defaultConfig.
      setDebugStockATPParameters(
        DebugParameters(true, true, true,
          Map(
            FieldNames.nameWrongField -> "xxxxx"
          )
        ))
  }

  "DebugUtils.logDSDebug " must "not fail when debug mode config is set" taggedAs (UnitTestTag) in new DebugConfig {
    val dsDebug: Dataset[DebugTest] = sc.parallelize(
      List[DebugTest](
        DebugTest("1",1,1d,new Timestamp(1)),
        DebugTest("2",2,2d,new Timestamp(2))
      )
    ).toDS
    dsDebug.logDSDebug(nameOf(dsDebug))(debugConfig.getDebugStockATPParameters.get)
    dsDebug.logDSDebugExplain(nameOf(dsDebug))(debugConfig.getDebugStockATPParameters.get)
    dsDebug.transform(logDebugTransform(nameOf(dsDebug))(debugConfig.getDebugStockATPParameters.get))
  }

  it must "not fail when debug mode config with filters is set" taggedAs (UnitTestTag) in new DebugConfig {
    val dsDebug: Dataset[DebugTest] = sc.parallelize(
      List[DebugTest](
        DebugTest("1",1,1d,new Timestamp(1)),
        DebugTest("2",2,2d,new Timestamp(2))
      )
    ).toDS
    dsDebug.logDSDebug(nameOf(dsDebug))(debugConfigWithFilters.getDebugStockATPParameters.get)
    dsDebug.logDSDebugExplain(nameOf(dsDebug))(debugConfigWithFilters.getDebugStockATPParameters.get)
    dsDebug.transform(logDebugTransform(nameOf(dsDebug))(debugConfigWithFilters.getDebugStockATPParameters.get))
  }

  it must "not fail when debug mode config with filters is set and case class extends of Debugger trait" taggedAs (UnitTestTag) in new DebugConfig {
    val dsDebugWithDebugger: Dataset[DebugWithDebuggerTest] = sc.parallelize(
      List[DebugWithDebuggerTest](
        DebugWithDebuggerTest("1",1,1d,new Timestamp(1)),
        DebugWithDebuggerTest("2",2,2d,new Timestamp(2))
      )
    ).toDS
    dsDebugWithDebugger.logDSDebug(nameOf(dsDebugWithDebugger))(debugConfigWithMoreFilters.getDebugStockATPParameters.get)
    dsDebugWithDebugger.logDSDebugExplain(nameOf(dsDebugWithDebugger))(debugConfigWithFilters.getDebugStockATPParameters.get)
    dsDebugWithDebugger.transform(logDebugTransform(nameOf(dsDebugWithDebugger))(debugConfigWithMoreFilters.getDebugStockATPParameters.get))
  }

  it must "not fail when debug mode config with filters is set and dataset is empty" taggedAs (UnitTestTag) in new DebugConfig {
    val dsDebug: Dataset[DebugTest] = sc.parallelize(
      List.empty[DebugTest]).toDS
    dsDebug.logDSDebug(nameOf(dsDebug))(debugConfigWithFilters.getDebugStockATPParameters.get)
    dsDebug.logDSDebugExplain(nameOf(dsDebug))(debugConfigWithFilters.getDebugStockATPParameters.get)
    dsDebug.transform(logDebugTransform(nameOf(dsDebug))(debugConfigWithFilters.getDebugStockATPParameters.get))

    val dsDebugWithDebugger: Dataset[DebugWithDebuggerTest] = sc.parallelize(
      List.empty[DebugWithDebuggerTest]).toDS
    dsDebugWithDebugger.logDSDebug(nameOf(dsDebugWithDebugger))(debugConfigWithMoreFilters.getDebugStockATPParameters.get)
    dsDebugWithDebugger.logDSDebugExplain(nameOf(dsDebugWithDebugger))(debugConfigWithFilters.getDebugStockATPParameters.get)
    dsDebugWithDebugger.transform(logDebugTransform(nameOf(dsDebugWithDebugger))(debugConfigWithMoreFilters.getDebugStockATPParameters.get))
  }

  it must "not fail when debug mode config with empty filters" taggedAs (UnitTestTag) in new DebugConfig {
    val dsDebug: Dataset[DebugTest] = sc.parallelize(
      List.empty[DebugTest]).toDS
    dsDebug.logDSDebug(nameOf(dsDebug))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get)
    dsDebug.logDSDebugExplain(nameOf(dsDebug))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get)
    dsDebug.transform(logDebugTransform(nameOf(dsDebug))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get))

    val dsDebugWithDebugger: Dataset[DebugWithDebuggerTest] = sc.parallelize(
      List.empty[DebugWithDebuggerTest]).toDS
    dsDebugWithDebugger.logDSDebug(nameOf(dsDebugWithDebugger))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get)
    dsDebugWithDebugger.logDSDebugExplain(nameOf(dsDebugWithDebugger))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get)
    dsDebugWithDebugger.transform(logDebugTransform(nameOf(dsDebugWithDebugger))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get))
  }

  it must "not fail when debug mode config with wrong filters" taggedAs (UnitTestTag) in new DebugConfig {
    val dsDebug: Dataset[DebugTest] = sc.parallelize(
      List.empty[DebugTest]).toDS
    dsDebug.logDSDebug(nameOf(dsDebug))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get)
    dsDebug.logDSDebugExplain(nameOf(dsDebug))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get)
    dsDebug.transform(logDebugTransform(nameOf(dsDebug))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get))

    val dsDebugWithDebugger: Dataset[DebugWithDebuggerTest] = sc.parallelize(
      List.empty[DebugWithDebuggerTest]).toDS
    dsDebugWithDebugger.logDSDebug(nameOf(dsDebugWithDebugger))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get)
    dsDebugWithDebugger.logDSDebugExplain(nameOf(dsDebugWithDebugger))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get)
    dsDebugWithDebugger.transform(logDebugTransform(nameOf(dsDebugWithDebugger))(debugConfigWithEmptyFilters.getDebugStockATPParameters.get))
  }
}
