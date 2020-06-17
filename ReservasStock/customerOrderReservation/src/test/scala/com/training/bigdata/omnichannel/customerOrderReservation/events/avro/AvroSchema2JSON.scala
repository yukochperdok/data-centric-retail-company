package com.training.bigdata.omnichannel.customerOrderReservation.events.avro

import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger
import org.apache.avro.{RandomData, Schema}

import scala.io.Source

object AvroSchema2JSON {

  def generateRandomJSON(schema: Schema, countJson: Int = 1): Seq[AnyRef] = {
    import scala.collection.JavaConverters._
    (new RandomData(schema, countJson).iterator()).asScala.toList
  }
}

object AvroSchema2JSONMain extends App with FunctionalLogger{

  if (args.isEmpty) {
    throw new Error(s"Runner needs to be called with the file.avsc.")
  }

  val avroSchemaFile = args.drop(0).head
  logConfig(s"The given avroSchemaFile is $avroSchemaFile")

  val avroSchema = new Schema.Parser().parse(Source.fromURL(getClass.getResource(s"/$avroSchemaFile")).mkString)

  val linesJson = AvroSchema2JSON.generateRandomJSON(avroSchema, 100).mkString(",\n")
  println(s"[$linesJson]")
}
