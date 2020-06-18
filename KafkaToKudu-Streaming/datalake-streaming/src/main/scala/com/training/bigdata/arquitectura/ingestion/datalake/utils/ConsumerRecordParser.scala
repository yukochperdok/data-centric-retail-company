package com.training.bigdata.arquitectura.ingestion.datalake.utils

import com.training.bigdata.arquitectura.ingestion.datalake.streams.{DataException, DataObject}
import com.training.bigdata.arquitectura.ingestion.datalake.streams.writer._
import io.circe.{Json, JsonNumber, JsonObject}
import io.circe.parser.parse
import org.apache.kafka.clients.consumer.ConsumerRecord

object ConsumerRecordParser {

  def parseConsumerRecord(consumerRecord: ConsumerRecord[String, String]): Either[ParsingException, (String, Operation, DataObject)] = {
    parse(consumerRecord.value) match {
      case Right(json) =>
        (json.hcursor.get[String]("table"), json.hcursor.get[String]("event"), json.hcursor.get[JsonObject]("data")) match {
          case (Right(table), Right(event), Right(data)) =>
            getOperation(event) match {
              case Some(operation) => Right((table, operation, new JsonDataObject(data.toMap)))
              case _ => Left(new ParsingException(s"Operation: ${event} - Table: ${table} - Data: ${data}"))
            }
          case _ => Left(new ParsingException(s"JSON format: ${consumerRecord.value}"))
        }
      case Left(_) => Left(new ParsingException(s"JSON format: ${consumerRecord.value}"))
    }
  }

  def getOperation(event: String): Option[Operation] = {
    event.toLowerCase match {
      case "u" => Option(UPSERT)
      case "i" => Option(INSERT)
      case "x" => Option(UPDATE)
      case "d" => Option(DELETE)
      case _ => Option.empty
    }
  }


  private class JsonDataObject(map: Map[String, Json]) extends DataObject {

    def size() = map.size

    def getKeys() = map.keys


    def +(value: (String, Any)): DataObject = {
      new JsonDataObject(map + (value._1 -> jsonFromStringOrLong(value._2)))
    }

    def ++(values: Map[String, Any]): DataObject = {
      new JsonDataObject(map ++ values.map(e => e._1 -> jsonFromStringOrLong(e._2)))
    }


    def isNullAt(key: String) = getJson(key).isNull

    def getString(key: String) = getJson(key).asString match {
      case Some(string) => string
      case _ => throw DataException(s"'${key}' is NOT a String")
    }

    def getByte(key: String) = getJsonNumber(key).toByte match {
      case Some(byte) => byte
      case _ => throw DataException(s"'${key}' is NOT a Byte")
    }

    def getShort(key: String) = getJsonNumber(key).toShort match {
      case Some(short) => short
      case _ => throw DataException(s"'${key}' is NOT a Short")
    }

    def getInt(key: String) = getJsonNumber(key).toInt match {
      case Some(int) => int
      case _ => throw DataException(s"'${key}' is NOT an Int")
    }

    def getLong(key: String) = getJsonNumber(key).toLong match {
      case Some(long) => long
      case _ => throw DataException(s"'${key}' is NOT a Long")
    }

    def getDouble(key: String) = getJsonNumber(key).toDouble

    def getBoolean(key: String) = getJson(key).asBoolean match {
      case Some(boolean) => boolean
      case _ => throw DataException(s"'${key}' is NOT a Boolean")
    }

    def getStringOrLong(key: String): Any = {
      val json = getJson(key)
      json.isNumber match {
        case true => json.asNumber.get.toLong match {
          case Some(long) => long
          case _ => throw DataException(s"'${key}' is NOT a String NOR a Long")
        }
        case _ => json.isString match {
          case true => json.asString.get
          case _ => throw DataException(s"'${key}' is NOT a String NOR a Long")
        }
      }
    }

    override def toString(): String = {
      map.toString()
    }


    private def getJson(key: String): Json = {
      map.get(key) match {
        case Some(json) => json
        case _ => throw DataException(s"Missing data for key: '${key}'")
      }
    }

    private def getJsonNumber(key: String): JsonNumber = {
      val json = getJson(key)
      json.asNumber match {
        case Some(number) => number
        case _ => throw DataException(s"'${key}' is NOT a number")
      }
    }

    private def jsonFromStringOrLong(value: Any): Json = {
      value match {
        case string: String => Json.fromString(string)
        case long: Long => Json.fromLong(long)
        case _ => throw DataException(s"Json constructor for type ${value.getClass} is NOT implemented")
      }
    }

  }

}
