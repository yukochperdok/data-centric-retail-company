package com.training.bigdata.omnichannel.customerOrderReservation.events.avro

import com.training.bigdata.omnichannel.customerOrderReservation.events.CustomerOrderReservationEvent.StringNullable
import com.training.bigdata.omnichannel.customerOrderReservation.events.CustomerOrderReservationEvent.{Order => OrderMain}
import com.training.bigdata.omnichannel.customerOrderReservation.utils.tag.TagTest.UnitTestTag
import org.apache.avro.Schema.Parser
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source
import scala.util.Try

trait GenericRecordMaker{
  def buildGenericRecord(schema: Schema): GenericRecord
}

case class User(id: Int, name: String, email: Option[String]) extends GenericRecordMaker {
  override def buildGenericRecord(userSchema: Schema): GenericRecord = {
    println(userSchema)

    val user: GenericRecord = new GenericData.Record(userSchema)
    user.put("id", id)
    user.put("name", name)
    user.put("email", email.orNull)
    user
  }
}

case class UserWrong(id: Int, name: String) extends GenericRecordMaker {
  override def buildGenericRecord(userSchema: Schema): GenericRecord = {
    println(userSchema)

    val user: GenericRecord = new GenericData.Record(userSchema)
    user.put("id", id)
    user.put("name", name)
    user
  }
}


case class Adjustment(id: String,promotionType: StringNullable) extends GenericRecordMaker{
  override def buildGenericRecord(adjustmentSchema: Schema): GenericRecord = {
    println(s"adjustmentSchema:$adjustmentSchema")

    val adjustment: GenericRecord = new GenericData.Record(adjustmentSchema)
    adjustment.put("id", id)
    adjustment.put("promotionType", promotionType.orNull)
    adjustment
  }
}

case class Order(commerceItems: List[CommerceItem]) extends GenericRecordMaker{
  override def buildGenericRecord(orderSchema: Schema): GenericRecord = {
    println(s"orderSchema:$orderSchema")

    val commerceItemSchema: Schema = orderSchema.getField("commerceItems").schema.getElementType
    val order: GenericRecord = new GenericData.Record(orderSchema)

    val listCommerceItems = new java.util.ArrayList[GenericRecord]
    commerceItems.foreach(commerceItem => listCommerceItems.add(commerceItem.buildGenericRecord(commerceItemSchema)))

    order.put("commerceItems", listCommerceItems)
    order
  }
}
case class CommerceItem(
  paymentGroupId: StringNullable,
  color: StringNullable,
  quantity: Int,
  volumetry: Volumetry,
  commerceItemPriceInfo: CommerceItemPriceInfo,
  adjustments: Option[List[Adjustment]]) extends GenericRecordMaker{

  override def buildGenericRecord(commerceItemSchema: Schema): GenericRecord = {
    println(s"commerceItemSchema:$commerceItemSchema")

    val commerceItem: GenericRecord = new GenericData.Record(commerceItemSchema)
    commerceItem.put("paymentGroupId", paymentGroupId.orNull)
    commerceItem.put("color", color.orNull)
    commerceItem.put("quantity", quantity)
    commerceItem.put("volumetry", volumetry.buildGenericRecord(commerceItemSchema.getField("volumetry").schema))
    commerceItem.put("commerceItemPriceInfo",
      commerceItemPriceInfo.buildGenericRecord(commerceItemSchema.getField("commerceItemPriceInfo").schema)
    )
    adjustments match {
      case None => commerceItem.put("adjustments", null)
      case Some(list) => {
        val adjustmentSchema: Schema = commerceItemSchema.getField("adjustments").schema.getTypes.get(1).getElementType
        val listAdjustment = new java.util.ArrayList[GenericRecord]
        list.foreach(elemList => listAdjustment.add(elemList.buildGenericRecord(adjustmentSchema)))
        commerceItem.put("adjustments", listAdjustment)
      }
    }
    commerceItem
  }
}

case class Volumetry(width: Option[Double], high: Option[Double]) extends GenericRecordMaker{

  override def buildGenericRecord(volmetrySchema: Schema): GenericRecord = {
    println(s"volmetrySchema:$volmetrySchema")

    val volumetry: GenericRecord = new GenericData.Record(volmetrySchema)
    volumetry.put("width", width.getOrElse(null))
    volumetry.put("high", high.getOrElse(null))
    volumetry
  }
}

case class CommerceItemPriceInfo(
  amountNoDiscounts: Option[Double],
  iva: Double,
  adjustments: Option[List[Adjustment]]) extends GenericRecordMaker{

  override def buildGenericRecord(commerceItemPriceInfoSchema: Schema): GenericRecord = {
    println(s"commerceItemPriceInfoSchema:$commerceItemPriceInfoSchema")

    val commerceItemPriceInfo: GenericRecord = new GenericData.Record(commerceItemPriceInfoSchema)
    commerceItemPriceInfo.put("amountNoDiscounts", amountNoDiscounts.getOrElse(null))
    commerceItemPriceInfo.put("iva", iva)
    adjustments match {
      case None => commerceItemPriceInfo.put("adjustments", null)
      case Some(list) => {
        val adjustmentSchema: Schema = commerceItemPriceInfoSchema.getField("adjustments").schema.getTypes.get(1).getElementType
        val listAdjustment = new java.util.ArrayList[GenericRecord]
        list.foreach(elemList => listAdjustment.add(elemList.buildGenericRecord(adjustmentSchema)))
        commerceItemPriceInfo.put("adjustments", listAdjustment)
      }
    }
    commerceItemPriceInfo
  }
}


class BeanToRecordConverterTest extends FlatSpec with Matchers {


  "BeanToRecordConverter" must "convert simple bean to generic record" taggedAs UnitTestTag in {

    val userSchema = new Parser().parse(Source.fromURL(getClass.getResource(s"/testUserSchema.avsc")).mkString)

    val user1: User = User(1, "name1", Some("email1"))
    val user2: User = User(1, "name1", None)

    val beanToRecordConverter = new BeanToRecordConverter[User](userSchema)

    val rcUser1 = beanToRecordConverter.convert(user1)
    println(rcUser1)
    assert(rcUser1 == user1.buildGenericRecord(userSchema))

    val rcUser2 = beanToRecordConverter.convert(user2)
    println(rcUser2)
    assert(rcUser2 == user2.buildGenericRecord(userSchema))

  }

  it must "warn of exisiting wrong schema" taggedAs UnitTestTag in {

    val userSchema = new Parser().parse(Source.fromURL(getClass.getResource(s"/testUserWrongSchema.avsc")).mkString)

    val user1: User = User(1, "name1", Some("email1"))
    val beanToRecordConverter = new BeanToRecordConverter[User](userSchema)

    val caught = intercept[RuntimeException] {
      beanToRecordConverter.convert(user1)
    }
    info(caught.getMessage)
    assert(caught.getMessage.indexOf("key not found: surname") != -1)

  }

  it must "warn of exisiting wrong bean" taggedAs UnitTestTag in {

    val userSchema = new Parser().parse(Source.fromURL(getClass.getResource(s"/testUserSchema.avsc")).mkString)

    val user1: UserWrong = UserWrong(1, "name1")
    val beanToRecordConverter = new BeanToRecordConverter[UserWrong](userSchema)

    val caught = intercept[RuntimeException] {
      beanToRecordConverter.convert(user1)
    }
    info(caught.getMessage)
    assert(caught.getMessage.indexOf("key not found: email") != -1)

  }

  it must "convert bean with arrays, complex records and nested types to generic record" taggedAs UnitTestTag in {

    val orderSimpleSchema = new Parser().parse(Source.fromURL(getClass.getResource(s"/testOrderSimpleSchema.avsc")).mkString)

    val volumetry: Volumetry = Volumetry(Some(23.5), None)
    val adj1: Adjustment = Adjustment("1",Some("adj1"))
    val adj2: Adjustment = Adjustment("2",Some("adj2"))
    val commerceItemPriceInfo: CommerceItemPriceInfo = CommerceItemPriceInfo(Some(5.0d), 21.0d,Some(adj1::Nil))
    val commerceItemPriceInfo2: CommerceItemPriceInfo = CommerceItemPriceInfo(None, 10.0d, None)
    val commerceItem1 = CommerceItem(
      Some("payment1"),
      Some("blue"),
      3,
      volumetry,
      commerceItemPriceInfo,
      Some(adj1 :: adj2 :: Nil)
    )

    val commerceItem2 = CommerceItem(
      None,
      Some("orange"),
      5,
      volumetry,
      commerceItemPriceInfo2,
      None
    )
    val order = Order(List(commerceItem1, commerceItem2))

    val beanToRecordConverter = new BeanToRecordConverter[Order](orderSimpleSchema)

    val rcOrder = beanToRecordConverter.convert(order)
    assert(rcOrder == order.buildGenericRecord(orderSimpleSchema))
  }

  it must "convert complex order json with random data" taggedAs UnitTestTag in {

    val orderSimpleSchema = new Parser().parse(Source.fromURL(getClass.getResource(s"/testOrderComplexSchema.avsc")).mkString)
    val beanToRecordConverter = new BeanToRecordConverter[OrderMain](orderSimpleSchema)

    import io.circe.generic.auto._
    import io.circe.parser
    assert{
      AvroSchema2JSON.generateRandomJSON(orderSimpleSchema, 1000)
        .forall {
          orderJson =>
            parser.decode[OrderMain](orderJson.toString) match {
              case Right(orderJsonBean) =>
                Try {
                  beanToRecordConverter.convert(orderJsonBean)
                  true
                }.getOrElse(false)
              case Left(_) => false
            }
        } == true
    }

  }
}
