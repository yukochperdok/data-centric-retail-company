package com.training.bigdata.arquitectura.ingestion.datalake.streams

import java.util.Date

import com.training.bigdata.arquitectura.ingestion.datalake.Record
import com.training.bigdata.arquitectura.ingestion.datalake.streams.writer.{DELETE, INSERT, UPDATE, UPSERT}
import com.training.bigdata.arquitectura.ingestion.datalake.utils.ConsumerRecordParser._
import com.training.bigdata.ingestion.utils.FunctionalLogger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD


class RecordStreamMapper(datalakeUser: String, mappings: Map[String, String], histTableList:List[String],
                         kuduMaxNumberOfColumns: Int) extends Serializable with FunctionalLogger{



  def map(rdd: RDD[ConsumerRecord[String, String]]): RDD[Record] = {

    rdd.flatMap[Record](r => {

      val id = s"${r.topic}-${r.partition}:${r.offset}"

      parseConsumerRecord(r) match {

        case Right((table, operation, data)) =>

          val kuduTable = s"${mappings.get(r.topic()).get}.${table.toLowerCase}"

          val currentTime = System.currentTimeMillis

          val audit = operation match {
            case UPSERT | INSERT =>
              Map[String, Any](
                "user_insert_dlk" -> datalakeUser, "ts_insert_dlk" -> currentTime,
                "user_update_dlk" -> datalakeUser, "ts_update_dlk" -> currentTime
              )
            case UPDATE =>
              Map[String, Any](
                "user_update_dlk" -> datalakeUser, "ts_update_dlk" -> currentTime
              )
            case DELETE => Map.empty[String, Any]
          }

          data.size <= kuduMaxNumberOfColumns match {
            case true =>
              table.toLowerCase match {
                case "marc" =>
                  val marcData = (data + ("date_write_event" -> r.timestamp)) ++ audit
                  List(Record(id, r.timestamp, kuduTable, operation, marcData))

                case _ =>  histTableList.contains(kuduTable.toLowerCase) match {
                  case true =>
                    val uuid = generateUUID(id, r.timestamp())

                    val dataHist = Map[String, Any](
                      "uuid" -> uuid, "event" -> operation.toString.toLowerCase,
                      "user_insert_dlk" -> datalakeUser, "ts_insert_dlk" -> currentTime,
                      "user_update_dlk" -> datalakeUser, "ts_update_dlk" -> currentTime)

                      List(
                        Record(id, r.timestamp, kuduTable, operation, data ++ audit),
                        Record(id, r.timestamp, kuduTable + "_h", UPSERT, data ++ dataHist)
                      )

                  case _ => List(Record(id, r.timestamp, kuduTable, operation, data ++ audit))
                }

              }
            case _ =>
                List(
                  Record(id, r.timestamp, kuduTable, operation, data ++ audit),
                  Record(id, r.timestamp, kuduTable + "_t002", operation, data ++ audit)
                )
          }

        case Left(exception) =>
          error(s"Record '${id}' (${new Date(r.timestamp)}) -> ${exception.message}",exception)
          List.empty[Record]

      }

    })

  }

  def generateUUID(id: String, timestamp: Long): String = {
    val uuid = id + "_" + timestamp
    uuid
  }

}
