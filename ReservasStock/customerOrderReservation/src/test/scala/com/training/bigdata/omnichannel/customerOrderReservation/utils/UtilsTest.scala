package com.training.bigdata.omnichannel.customerOrderReservation.utils

import com.training.bigdata.omnichannel.customerOrderReservation.utils.tag.TagTest.UnitTestTag
import org.scalatest.{FlatSpecLike, Matchers}

class UtilsTest extends FlatSpecLike with Matchers {

  "logAlertWhenNoneAndReturn" must " return the value it receives" taggedAs (UnitTestTag) in {
    val v1 = Some("sth")
    val v2 = None
    Utils.logAlertWhenNoneAndReturn(v1, "some log") shouldBe v1
    Utils.logAlertWhenNoneAndReturn(v2, "some log") shouldBe v2
  }

  "StringUtils.lpad" must " fill the length of string by left-side with a character" taggedAs UnitTestTag in {
    import com.training.bigdata.omnichannel.customerOrderReservation.utils.Utils.StringUtils

    val code1: String = "11111"
    val code2: String = "2"
    val code3: String = ""
    code1.lpad(13, '0') shouldBe "00000000"+code1
    code2.lpad(13, '0') shouldBe "000000000000"+code2
    code3.lpad(13, '0') shouldBe "0000000000000"+code3
  }

  it must " fill properly the length of a string in corner cases" taggedAs UnitTestTag in {
    import com.training.bigdata.omnichannel.customerOrderReservation.utils.Utils.StringUtils

    val code1: String = "11111"
    code1.lpad(0, '0') shouldBe code1
    code1.lpad(-1, '0') shouldBe code1
  }

}
