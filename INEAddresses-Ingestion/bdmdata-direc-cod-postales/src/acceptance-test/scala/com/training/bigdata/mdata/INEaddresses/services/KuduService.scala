package com.training.bigdata.mdata.INEaddresses.services

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.training.bigdata.mdata.INEaddresses.tables.MockTables
import org.apache.kudu.client.KuduPredicate.ComparisonOp.EQUAL
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder
import org.apache.kudu.client._
import org.apache.kudu.{ColumnSchema, Schema, Type}

import scala.collection.JavaConverters._
import scala.io.Source

object KuduService extends MockTables {

  var kuduClient: KuduClient = _

  def closeKudu(): Unit = kuduClient match {
    case _: KuduClient =>
      kuduClient.close()
    case _ =>
  }

  def initKudu: Unit = kuduClient = new KuduClient.KuduClientBuilder("localhost:7051").build()

  def createInitialKudu(tablesToCreate: LoadFunctions)(implicit specificLocationPath: String): Unit = {

    println("###############################################################")
    println(s"##########  KUDU   SCENARIO: $specificLocationPath ########################")
    println("###############################################################")

    applyFuncTables(tablesToCreate)

  }

  def loadInitialKudu(tablesToload: LoadFunctions)(implicit specificLocationPath: String): Unit = {
    println("###############################################################")
    println(s"##########  KUDU   SCENARIO: $specificLocationPath ########################")
    println("###############################################################")

    applyFuncTables(tablesToload)
  }

  def cleanKuduTables: Unit = kuduClient.getTablesList().getTablesList.asScala.map(kuduClient.deleteTable(_))


  lazy val createKuduTable: (String, String) => () => Unit =
    (tableName, tableSchema) => {
      val schema = new Schema(getColumnsSchemas(tableSchema).asJava)
      val createTableOptions = new CreateTableOptions
      createTableOptions.setNumReplicas(1)
      createTableOptions.addHashPartitions(getPrimaryKeyColumns(schema).asJava, 3)
      kuduClient.createTable(tableName, schema, createTableOptions)

      () => Unit
    }


  lazy val loadKuduTable: (String, String) => () => Unit =
    (tableName, data) => {
      val session: KuduSession = kuduClient.newSession
      val table = kuduClient.openTable(tableName)
      val timestampExtractor = TimestampExtractor

      val lines = Source.fromFile(data).getLines.toList
      val header = lines.head.split(",").zipWithIndex.toMap

      lines.drop(1).foreach(line => {
        val operation = table.newInsert()
        val values = line.split(",")

        for (c: ColumnSchema <- table.getSchema.getColumns.asScala) {
          val name = c.getName
          val value = values(header(name))

          value.equalsIgnoreCase("NULL") match {
            case true =>
              c.isKey || !c.isNullable match {
                case true => throw new IllegalArgumentException(s"Found NULL value in not nullable column! : [column: $name]")
                case _ => operation.getRow.setNull(name)
              }
            case false =>
              c.getType match {
                case Type.STRING => operation.getRow.addString(name, value)
                case Type.INT32 => operation.getRow.addInt(name, value.toInt)
                case Type.INT64 => operation.getRow.addLong(name, value.toLong)
                case Type.DOUBLE => operation.getRow.addDouble(name, value.toDouble)
                case Type.BOOL => operation.getRow.addBoolean(name, value.toBoolean)
                case Type.UNIXTIME_MICROS => operation.getRow.addLong(name, timestampExtractor.getTimestamp(value))
                case _ => throw new IllegalArgumentException(s"Type doesn't found or not recognizeg. Type:  ${c.getType}")
              }
          }

        }
        session.apply(operation)
      })

      session.close()

      () => Unit
    }

  private def getColumnsSchemas(file: String): List[ColumnSchema] = {
    val lines = Source.fromFile(file).getLines.toList
    val header = lines.head.split(",").zipWithIndex.toMap

    lines.drop(1).map(line => {
      val values = line.split(",")
      new ColumnSchema.ColumnSchemaBuilder(values(header("name")), getType(values(header("type"))))
        .key(values(header("primary_key")).toBoolean).nullable(values(header("nullable")).toBoolean)
        .build
    })
  }

  private def getPrimaryKeyColumns(schema: Schema): List[String] = {
    schema.getPrimaryKeyColumns.asScala.map(_.getName).toList
  }

  private def getType(kuduType: String): Type = {
    kuduType match {
      case "string" => Type.STRING
      case "boolean" => Type.BOOL
      case "int" => Type.INT32
      case "bigint" => Type.INT64
      case "double" => Type.DOUBLE
      case "timestamp" => Type.UNIXTIME_MICROS
      case _ => throw new IllegalArgumentException(s"Type $kuduType doesn't found or not recognized")
    }
  }

  def countKuduRecords(tableName: String): Int = {
    val table = kuduClient.openTable(tableName)
    val scanner = kuduClient.newScannerBuilder(table).build()
    var count = 0
    while (scanner.hasMoreRows) {
      val results = scanner.nextRows()
      while (results.hasNext) {
        results.next()
      }
      count += results.getNumRows
    }
    scanner.close()
    count
  }

  def query[T](tableName: String, columns: Option[List[String]], predicates: Option[Map[String, String]], mapper: RowResult => T): List[T] = {
    val (scannerBuilder: KuduScannerBuilder, schema: Schema) = prepare(tableName, columns)

    predicates match {
      case Some(criteria) =>
        criteria.foreach(value => scannerBuilder.addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumn(value._1), EQUAL, value._2)))
      case None =>
    }

    val scanner: KuduScanner = scannerBuilder.build()

    val result = getResultRec(scanner, mapper).toList

    scanner.close()
    result
  }

  private def prepare[T](tableName: String, columns: Option[List[String]]): (KuduScannerBuilder, Schema) = {
    val table: KuduTable = kuduClient.openTable(tableName)
    val schema = table.getSchema
    val scannerBuilder: KuduScannerBuilder = kuduClient.newScannerBuilder(table)
    columns match {
      case Some(cols) => scannerBuilder.setProjectedColumnNames(cols.asJava)
      case _ =>
    }
    (scannerBuilder, schema)
  }

  private[this] def getResultRec[T](scanner: KuduScanner, mapper: RowResult => T): Seq[T] = {
    new Iterator[Stream[T]] {
      def hasNext: Boolean = scanner.hasMoreRows

      def next(): Stream[T] = getRowsRec[T](scanner.nextRows(), mapper)
    }.toSeq.flatten.reverse
  }

  private[this] def getRowsRec[T](rows: RowResultIterator, mapper: RowResult => T): Stream[T] = {
    new Iterator[T] {
      def hasNext: Boolean = rows.hasNext

      def next(): T = mapper(rows.next())
    }.toStream
  }

}

object TimestampExtractor {

  val dateTimeRegex = """\d{4}-\d\d-\d\d \d\d:\d\d:\d\d""".r
  val dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  dateTimeFormatter.setTimeZone(TimeZone.getTimeZone("Europe/Madrid"))

  val dateRegex = """\d{4}-\d\d-\d\d""".r
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
  dateFormatter.setTimeZone(TimeZone.getTimeZone("Europe/Madrid"))

  def getTimestamp(value: Any) : Long = {
    val milliseconds = value match {
      case long: Long => long
      case string: String => string match {
        case dateTimeRegex() => dateTimeFormatter.parse(string).getTime
        case dateRegex() => dateFormatter.parse(string).getTime
        case _ => throw new IllegalArgumentException(s"String '$value' cannot be converted to timestamp")
      }
      case _ => throw new IllegalArgumentException(s"Value '$value' cannot be converted to timestamp")
    }

    // UNIXTIME_MICROS column, the long value provided should be the number
    // of microseconds between a given time and January 1, 1970 UTC.
    milliseconds * 1000L
  }

  // Little disclaimer about that. This method is related with the 1.8.0 version of KuduClient.
  // The current version (1.6.0) has not the getTimestamp functionality.
  def getTimestampFromMicros(micros: Long): Timestamp = {
    import java.util.concurrent.TimeUnit
    val millis = TimeUnit.MILLISECONDS.convert(micros, TimeUnit.MICROSECONDS)
    new Timestamp(millis)
  }
}

