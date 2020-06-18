package com.training.bigdata.mdata.INEaddresses.logging

import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.spi.LoggingEvent

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CustomAppender {
  val listOfEvents: ListBuffer[LoggingEvent] = mutable.ListBuffer.empty[LoggingEvent]

  def clear(): Unit = listOfEvents.clear()
}

class CustomAppender extends AppenderSkeleton {
  import CustomAppender.listOfEvents

  override def append(loggingEvent: LoggingEvent): Unit = listOfEvents += loggingEvent

  override def close(): Unit = {}

  override def requiresLayout(): Boolean = false

  def getLog: List[LoggingEvent] = listOfEvents.toList

}