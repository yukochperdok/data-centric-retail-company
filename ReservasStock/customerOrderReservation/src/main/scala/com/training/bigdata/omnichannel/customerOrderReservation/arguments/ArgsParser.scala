package com.training.bigdata.omnichannel.customerOrderReservation.arguments

object ArgsParser {

  def getArguments(args: Array[String]): Args = {
    args.length match {
      case 0 =>
        throw new IllegalArgumentException("Params error: No user defined")
      case _ => Args(args.head)
    }
  }

}

case class Args(user: String)
