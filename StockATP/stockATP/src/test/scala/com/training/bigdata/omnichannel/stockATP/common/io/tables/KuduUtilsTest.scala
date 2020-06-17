package com.training.bigdata.omnichannel.stockATP.common.io.tables

import org.apache.kudu.spark.kudu.KuduContext
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class KuduUtilsTest extends FlatSpec with Matchers with MockitoSugar {

  "getTableName " must "return the impala:: table name" in {
    val fullTableName1 = "schema.table1"
    val fullImpalaTableName1 = "impala::schema.table1"
    implicit val kuduContext = mock[KuduContext]

    Mockito.doReturn(false).when(kuduContext).tableExists(fullTableName1)
    Mockito.doReturn(true).when(kuduContext).tableExists(fullImpalaTableName1)

    KuduUtils.getTableName(fullTableName1) shouldBe fullImpalaTableName1
  }

  it must "return the table name without impala::" in {
    val fullTableName1 = "schema.table1"
    val fullImpalaTableName1 = "impala::schema.table1"
    implicit val kuduContext = mock[KuduContext]

    Mockito.doReturn(true).when(kuduContext).tableExists(fullTableName1)
    Mockito.doReturn(false).when(kuduContext).tableExists(fullImpalaTableName1)

    KuduUtils.getTableName(fullTableName1) shouldBe fullTableName1
  }

  it must "return the table name with only one impala::" in {
    val fullTableName1 = "impala::schema.table1"
    val fullImpalaTableName1 = "impala::impala::schema.table1"
    implicit val kuduContext = mock[KuduContext]

    Mockito.doReturn(true).when(kuduContext).tableExists(fullTableName1)
    Mockito.doReturn(false).when(kuduContext).tableExists(fullImpalaTableName1)

    KuduUtils.getTableName(fullTableName1) shouldBe fullTableName1
  }
}
