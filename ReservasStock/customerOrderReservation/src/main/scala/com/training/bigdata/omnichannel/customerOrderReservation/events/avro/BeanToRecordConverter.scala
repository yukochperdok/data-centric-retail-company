package com.training.bigdata.omnichannel.customerOrderReservation.events.avro

import java.lang.reflect.Method
import java.util

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

object BeanToRecordConverterConversions {
  implicit def beanToGenericRecord[E](event: E)(implicit beanToRecordConverter: BeanToRecordConverter[E]): GenericRecord ={
    beanToRecordConverter.convert(event)
  }

}

class BeanToRecordConverter[E](schema: Schema) {
  def convert(bean: E): GenericRecord = try
    convertBeanToRecord(bean, schema)
  catch {
    case e: Exception =>
      throw new RuntimeException(e)
  }

  private[this] def isSimpleType(`type`: Schema.Type): Boolean = {
    `type` match {
      case Schema.Type.STRING |
           Schema.Type.INT |
           Schema.Type.LONG |
           Schema.Type.DOUBLE |
           Schema.Type.BOOLEAN => true
      case _ => false
    }
  }

  private[this] val optionToValue =
    (value: AnyRef) => {
      value match {
        case Some(s) => s
        case None => null
        case _ => value
      }
    }

  @throws[Exception]
  private[this] def convertBeanToRecord(bean: Any, schema: Schema): GenericData.Record = {
    val beanClass: Class[_] = bean.getClass
    val beanFields: List[String] = beanClass.getDeclaredFields.toList.map(x => x.getName)
    val getters: Map[String, Method] =
      beanClass
        .getDeclaredMethods.toList
        .filter(x => beanFields.contains(x.getName))
        .map(x => (x.getName, x))
        .toMap

    val result: GenericData.Record = new GenericData.Record(schema)
    val fields: util.List[Schema.Field] = schema.getFields

    import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._

    fields.foreach { field =>
      val fieldSchema: Schema = field.schema
      val `type`: Schema.Type = fieldSchema.getType
      val name: String = field.name
      val valueInvoked = getters(name).invoke(bean)
      val value = optionToValue(valueInvoked)

      if (isSimpleType(`type`)) {
        result.put(name, value)
      } else{
        (`type`: @unchecked) match {
          case Schema.Type.RECORD =>
            val fieldRes = convertBeanToRecord(value, fieldSchema)
            result.put(name, fieldRes)

          case Schema.Type.ARRAY =>
            val elements: List[_] = value.asInstanceOf[List[_]]
            val elementSchema: Schema = fieldSchema.getElementType

            if (isSimpleType(elementSchema.getType)) {
              result.put(name, elements.asJava)
            } else {
              val results = elements.map(element => convertBeanToRecord(element, elementSchema))
              result.put(name, results.asJava)
            }

          case Schema.Type.UNION =>
            val types: List[Schema.Type] = fieldSchema.getTypes.toList.map(x => x.getType)
            /* Not allowed differents both simple and non simple types, apart from the null type
               Assume taking head type
            */
            if (types.contains(Schema.Type.NULL)){
              val remainingUnionSchemas: Seq[Schema] = fieldSchema.getTypes.filterNot(_.getType == Schema.Type.NULL)

              if(remainingUnionSchemas.nonEmpty)
                if(isSimpleType(remainingUnionSchemas.head.getType) || (value == null)){
                  result.put(name, value)
                }
                else if(remainingUnionSchemas.head.getType == Schema.Type.ARRAY) {
                  val elements: List[_] = value.asInstanceOf[List[_]]
                  val elementSchema: Schema = remainingUnionSchemas.head.getElementType
                  if (isSimpleType(elementSchema.getType)) {
                    result.put(name, elements.asJava)
                  } else {
                    val results = elements.map(element => convertBeanToRecord(element, elementSchema))
                    result.put(name, results.asJava)
                  }
                } else
                  result.put(name, convertBeanToRecord(value, remainingUnionSchemas.head))
            } else if (types.size == 1)
              if (isSimpleType(types.head) || (value == null)) result.put(name, value)

        }
      }
    }
    result
  }
}