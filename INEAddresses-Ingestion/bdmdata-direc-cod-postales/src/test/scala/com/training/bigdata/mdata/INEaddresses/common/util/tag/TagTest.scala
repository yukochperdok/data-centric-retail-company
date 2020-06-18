package com.training.bigdata.mdata.INEaddresses.common.util.tag

object TagTest {

  import org.scalatest.Tag

  object UnitTestTag extends Tag("UTest")
  object IntegrationTestTag extends Tag("ITest")
  object AcceptanceTestsSuite extends Tag("QATest")

}
