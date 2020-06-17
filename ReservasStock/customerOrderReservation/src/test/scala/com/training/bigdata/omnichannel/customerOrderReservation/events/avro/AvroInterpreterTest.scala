package com.training.bigdata.omnichannel.customerOrderReservation.events.avro

import com.training.bigdata.omnichannel.customerOrderReservation.utils.{DefaultConf, SparkLocalTest}
import com.training.bigdata.omnichannel.customerOrderReservation.utils.tag.TagTest.UnitTestTag
import io.circe.parser
import io.circe.generic.auto._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source
import scala.util.Random

class AvroInterpreterTest extends FlatSpec with Matchers with SparkLocalTest with DefaultConf {

  def generateRDDFromJson(jsonPath: String, schema: Schema) : RDD[ConsumerRecord[String, GenericRecord]] = {

    val jsonFile = Source.fromURL(getClass.getResource(jsonPath))

    parser.decode[List[User]](jsonFile.mkString) match {
      case Right(listOrderJsonBean) =>

        val records = listOrderJsonBean.map {
          event => {
            new ConsumerRecord[String, GenericRecord](
              "topic", 1, 0, Random.nextString(10),
              new BeanToRecordConverter[User](schema).convert(event))
          }
        }
        jsonFile.close
        sparkContext.parallelize(records)

      case Left(error) =>
        jsonFile.close
        fail(s"Could not get input rdd for test because of: $error")
    }

  }

  "parseAvroToDF" must "parse a generic record rdd to a dataframe" taggedAs UnitTestTag in {

    val schema = new Schema.Parser().parse(Source.fromURL(getClass.getResource("/testUserSchema.avsc")).mkString)

    val orderInputRDD: RDD[ConsumerRecord[String, GenericRecord]] =
      generateRDDFromJson("/inputJsons/testUser.json", schema)

    val expectedSchema = StructType(List(
      StructField(    "id", IntegerType, false),
      StructField(  "name",  StringType, false),
      StructField( "email",  StringType, true)
    ))

    val expectedDF = sparkSession.createDataFrame(sparkContext.parallelize(List[Row](
      Row(1, "Carlos", "carlos@soyestoi.co")
    )), expectedSchema)

    val resultDF = AvroInterpreter.parseAvroToDF(orderInputRDD, schema)

    assertDataFrameEquals(expectedDF, resultDF)
  }

  "explodeColumnsAndSelectFields" must "explode an structure and select fields inside and aout of that structure" taggedAs UnitTestTag in {

    val inputSchema = StructType(List(
      StructField(               "id", IntegerType, false),
      StructField(             "name",  StringType, false),
      StructField( "listToBeExploded", ArrayType(
        StructType(List(
          StructField(       "fieldInside1", StringType, false),
          StructField("fieldInsideNullable", StringType, true)
        )), false), false))
    )

    val inputDF = sparkSession.createDataFrame(sparkContext.parallelize(List[Row](
      Row(1, "Carlos", Array(Row("valueInside1", "notNullValue"), Row("valueInside2", null)))
    )), inputSchema)

    val resultDF = AvroInterpreter.explodeColumnsAndSelectFields(inputDF,
      "listToBeExploded",
      List("listToBeExploded.fieldInside1", "listToBeExploded.fieldInsideNullable"),
      List("id")
    )

    val expectedSchema = StructType(List(
      StructField(  "id", IntegerType, false),
      StructField("fieldInside1",  StringType, false),
      StructField("fieldInsideNullable",  StringType, true)
    ))

    val expectedDF = sparkSession.createDataFrame(sparkContext.parallelize(List[Row](
      Row(1, "valueInside1", "notNullValue"),
      Row(1, "valueInside2", null)
    )), expectedSchema)

    assertDataFrameEquals(expectedDF, resultDF)
  }
}
