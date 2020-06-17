package com.training.bigdata.omnichannel.customerOrderReservation.producer.generator

import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.io.Source

object BuilderGenericRecord{
  def generateRecordTest1(rootSchema: Schema): GenericRecord = {
    // Create avro generic record object
    println(rootSchema)

    val order: GenericRecord = new GenericData.Record(rootSchema)

    val commerceItemsSchema: Schema = order.getSchema.getField("commerceItems").schema
    println(s"commerceItemsSchema $commerceItemsSchema")
    val commerceItemSchema: Schema = commerceItemsSchema.getElementType
    println(s"commerceItemSchema $commerceItemSchema")
    val commerceItemPriceInfoSchema: Schema = commerceItemSchema.getField("commerceItemPriceInfo").schema
    println(s"commerceItemPriceInfoSchema $commerceItemPriceInfoSchema")
    val volumetrySchema: Schema = commerceItemSchema.getField("volumetry").schema
    println(s"volumetrySchema $volumetrySchema")


    val commerceItem: GenericRecord = new GenericData.Record(commerceItemSchema)
    val commerceItemPriceInfo: GenericRecord = new GenericData.Record(commerceItemPriceInfoSchema)
    val volumetry: GenericRecord = new GenericData.Record(volumetrySchema)


    //volumetry
    volumetry.put("width", 1.0)
    volumetry.put("high", null)


    //commerceItemPriceInfo
    commerceItemPriceInfo.put("amountNoDiscounts", 1.0)
    commerceItemPriceInfo.put("iva", 21.0)


    //CommerceItem
    commerceItem.put("paymentGroupId", "paymentGroupId1")
    commerceItem.put("color", "rojo")
    commerceItem.put("quantity", 3)
    commerceItem.put("volumetry", volumetry)
    commerceItem.put("commerceItemPriceInfo", commerceItemPriceInfo)


    val commerceItems = new java.util.ArrayList[GenericRecord]
    commerceItems.add(commerceItem)

    order.put("commerceItems", commerceItems)

    println(s"Order: $order")
    order
  }

  def generateRecordTest2(rootSchema: Schema): GenericRecord = {
    // Create avro generic record object
    println(rootSchema)

    val order: GenericRecord = new GenericData.Record(rootSchema)

    val commerceItemsSchema: Schema = order.getSchema.getField("commerceItems").schema
    println(s"commerceItemsSchema $commerceItemsSchema")
    val commerceItemSchema: Schema = commerceItemsSchema.getElementType
    println(s"commerceItemSchema $commerceItemSchema")
    val commerceItemPriceInfoSchema: Schema = commerceItemSchema.getField("commerceItemPriceInfo").schema
    println(s"commerceItemPriceInfoSchema $commerceItemPriceInfoSchema")
    val volumetrySchema: Schema = commerceItemSchema.getField("volumetry").schema
    println(s"volumetrySchema $volumetrySchema")


    val commerceItem1: GenericRecord = new GenericData.Record(commerceItemSchema)
    val commerceItem2: GenericRecord = new GenericData.Record(commerceItemSchema)
    val commerceItem3: GenericRecord = new GenericData.Record(commerceItemSchema)
    val commerceItemPriceInfo1: GenericRecord = new GenericData.Record(commerceItemPriceInfoSchema)
    val commerceItemPriceInfo2: GenericRecord = new GenericData.Record(commerceItemPriceInfoSchema)
    val commerceItemPriceInfo3: GenericRecord = new GenericData.Record(commerceItemPriceInfoSchema)
    val volumetry: GenericRecord = new GenericData.Record(volumetrySchema)


    //volumetry
    volumetry.put("width", 1.0)
    volumetry.put("high", null)


    //commerceItemPriceInfo
    commerceItemPriceInfo1.put("amountNoDiscounts", 1.0)
    commerceItemPriceInfo1.put("iva", 21.0)

    commerceItemPriceInfo2.put("amountNoDiscounts", 2.0)
    commerceItemPriceInfo2.put("iva", 19.0)

    commerceItemPriceInfo3.put("amountNoDiscounts", 3.0)
    commerceItemPriceInfo3.put("iva", 13.0)


    //CommerceItem
    commerceItem1.put("paymentGroupId", "paymentGroupId1")
    commerceItem1.put("color", "rojo")
    commerceItem1.put("quantity", 3)
    commerceItem1.put("volumetry", volumetry)
    commerceItem1.put("commerceItemPriceInfo", commerceItemPriceInfo1)


    commerceItem2.put("paymentGroupId", "paymentGroupId1")
    commerceItem2.put("color", "azul")
    commerceItem2.put("quantity", 60)
    commerceItem2.put("volumetry", volumetry)
    commerceItem2.put("commerceItemPriceInfo", commerceItemPriceInfo2)

    commerceItem3.put("paymentGroupId", "paymentGroupId1")
    commerceItem3.put("color", null)
    commerceItem3.put("quantity", 100)
    commerceItem3.put("volumetry", volumetry)
    commerceItem3.put("commerceItemPriceInfo", commerceItemPriceInfo3)


    val commerceItems = new java.util.ArrayList[GenericRecord]
    commerceItems.add(commerceItem1)
    commerceItems.add(commerceItem2)
    commerceItems.add(commerceItem3)

    order.put("commerceItems", commerceItems)

    println(s"Order: $order")
    order
  }

  def generateRecordTestListNull(rootSchema: Schema): GenericRecord = {
    // Create avro generic record object
    println(rootSchema)

    val order: GenericRecord = new GenericData.Record(rootSchema)

    val commerceItems = new java.util.ArrayList[GenericRecord]

    order.put("commerceItems", commerceItems)

    println(s"Order: $order")
    order
  }
}

object BuilderGenericRecordMain extends App with FunctionalLogger{

  val schema = new Parser().parse(Source.fromURL(getClass.getResource(s"/test.avsc")).mkString)
  logInfo("generateRecordTest1")
  BuilderGenericRecord.generateRecordTest1(schema)

  logInfo("generateRecordTest2")
  BuilderGenericRecord.generateRecordTest2(schema)

  logInfo("generateRecordTestListNull")
  BuilderGenericRecord.generateRecordTestListNull(schema)

}
