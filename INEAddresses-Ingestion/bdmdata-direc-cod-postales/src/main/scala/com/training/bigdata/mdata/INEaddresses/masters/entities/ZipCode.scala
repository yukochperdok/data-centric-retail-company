package com.training.bigdata.mdata.INEaddresses.masters.entities

object ZipCode {

  object dbTags {
    val zipCode = "zip_code"
    val townCode = "town_code"
    val countryCode = "land1"
    val region = "bland"
    val ccaaCode = "codauto"
    val modificationDateINE = "ine_modification_date"
  }

  val pkFields = Seq(dbTags.zipCode, dbTags.townCode, dbTags.countryCode)

}