package com.training.bigdata.arquitectura.ingestion.datalake.utils

import com.training.bigdata.arquitectura.ingestion.datalake.config.Topic
import com.training.bigdata.arquitectura.ingestion.datalake.streams.DataException
import com.training.bigdata.arquitectura.ingestion.datalake.streams.writer._
import org.apache.kudu.Schema

import collection.JavaConversions._


object CommonUtils {

  def getTopicToSchemaMappings(topics: List[Topic]): Map[String, String] = {
    topics.map(topic => (topic.name -> topic.schema)).toMap
  }

  def normalizeMapKeys(operation: Operation, originalKeys: Iterable[String], schema: Schema): Map[String, String] = {
    val columns = operation match {
      case UPSERT | INSERT => schema.getColumns.map(c => c.getName)
      case DELETE => schema.getPrimaryKeyColumns.map(c => c.getName)
      case UPDATE => schema.getColumns.map(c => c.getName).filter(!List("user_insert_dlk", "ts_insert_dlk").contains(_))
    }

    val normalizedKeys: Map[String, String] = originalKeys.map(entry => {
      entry.startsWith("/") match {
        case true => (entry.substring(1).replace("/", "_").toLowerCase, entry)
        case _ => (entry.toLowerCase, entry)
      }
    }).toMap.filterKeys(columns.contains(_))

    columns.foreach(name =>
      normalizedKeys.contains(name) match {
        case false => throw DataException("Data keys do NOT match table columns")
        case _ =>
      }
    )

    normalizedKeys
  }

}
