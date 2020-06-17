package com.training.bigdata.omnichannel.customerOrderReservation.arguments

import com.training.bigdata.omnichannel.customerOrderReservation.utils.tag.TagTest.UnitTestTag
import org.scalatest.{FlatSpecLike, Matchers}

class ArgsParserTest extends FlatSpecLike with Matchers {

  "getArguments" must " return a properties util with the user" taggedAs (UnitTestTag) in {
    val user = "user1"
    val args = Array(user)
    val arguments = ArgsParser.getArguments(args)
    arguments.user shouldBe user
  }

  "getArguments" must " exist " taggedAs (UnitTestTag) in {
    val args = Array[String]()
    val thrown = intercept[IllegalArgumentException] {
      ArgsParser.getArguments(args)
    }
    thrown.getMessage shouldBe "Params error: No user defined"
  }
}
