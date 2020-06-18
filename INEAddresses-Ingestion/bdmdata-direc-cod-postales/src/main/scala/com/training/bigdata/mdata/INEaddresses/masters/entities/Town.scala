package com.training.bigdata.mdata.INEaddresses.masters.entities


object Town {

  object dbTags {
    val townCode = "town_code"
    val countryCode = "land1"
    val language = "spras"
    val townName = "town_name"
    val region = "bland"
    val ccaaCode = "codauto"
    val modificationDateINE = "ine_modification_date"
  }

  val pkFields = Seq(dbTags.townCode, dbTags.countryCode, dbTags.language)

}