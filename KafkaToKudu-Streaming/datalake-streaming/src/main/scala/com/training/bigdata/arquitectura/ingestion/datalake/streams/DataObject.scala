package com.training.bigdata.arquitectura.ingestion.datalake.streams


case class DataException(message: String) extends Exception(message)


trait DataObject {

  def size(): Int


  def getKeys(): Iterable[String]


  def +(value: (String, Any)): DataObject

  def ++(values: Map[String, Any]): DataObject


  def isNullAt(key: String): Boolean

  def getString(key: String): String

  def getByte(key: String): Byte

  def getShort(key: String): Short

  def getInt(key: String): Int

  def getLong(key: String): Long

  def getDouble(key: String): Double

  def getBoolean(key: String): Boolean

  def getStringOrLong(key: String): Any


  def toString(): String

}
