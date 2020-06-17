package com.training.bigdata.omnichannel.customerOrderReservation.utils

import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger

object Utils extends FunctionalLogger{

  def logAlertWhenNoneAndReturn[T](opt: Option[T], message: String): Option[T] = {
    if(opt.isEmpty) logAlert(message)
    opt
  }

  implicit class StringUtils(string: String) {

    def lpad(length: Int, char2Prepend: Char): String = {
      if (string.length < length) string.reverse.padTo(length, char2Prepend).reverse else string
    }
  }

}
