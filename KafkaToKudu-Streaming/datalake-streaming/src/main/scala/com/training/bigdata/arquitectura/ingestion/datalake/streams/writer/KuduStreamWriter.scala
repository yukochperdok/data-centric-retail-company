package com.training.bigdata.arquitectura.ingestion.datalake.streams.writer

import java.security.{AccessController, PrivilegedAction}
import java.util.Date

import javax.security.auth.Subject
import javax.security.auth.login.{AppConfigurationEntry, Configuration, LoginContext}
import com.training.bigdata.arquitectura.ingestion.datalake.Record
import com.training.bigdata.arquitectura.ingestion.datalake.config.AppConfig
import com.training.bigdata.arquitectura.ingestion.datalake.streams.DataException
import com.training.bigdata.arquitectura.ingestion.datalake.utils.CommonUtils._
import com.training.bigdata.arquitectura.ingestion.datalake.utils.TimestampExtractor
import com.training.bigdata.ingestion.utils.{FunctionalLogger, LogType, MonitorType}
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.kudu.{ColumnSchema, Type}
import org.apache.kudu.client._
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.util.AccumulatorV2

import scala.collection.JavaConverters._
import scala.collection.mutable


sealed trait Operation
case object UPSERT extends Operation
case object INSERT extends Operation
case object UPDATE extends Operation
case object DELETE extends Operation


class KuduStreamWriter(appConfig: AppConfig, sc: SparkContext) extends Serializable with FunctionalLogger {

  @transient lazy val syncClient: KuduClient = asyncClient.syncClient()

  @transient lazy val asyncClient: AsyncKuduClient = {
    val c = KuduClientCache.getAsyncClient(appConfig.application.parameters("kuduHost"))
    if (authnCredentials != null) {
      c.importAuthenticationCredentials(authnCredentials)
    }
    c
  }

  private val authnCredentials : Array[Byte] = {
    Subject.doAs(KuduStreamWriter.getSubject(sc), new PrivilegedAction[Array[Byte]] {
      override def run(): Array[Byte] = syncClient.exportAuthenticationCredentials()
    })
  }


  /**
    * TimestampAccumulator accumulates the maximum value of client's
    * propagated timestamp of all executors and can only read by the
    * driver.
    */
  private class TimestampAccumulator(var timestamp: Long = 0L)
    extends AccumulatorV2[Long, Long] {
    override def isZero: Boolean = {
      timestamp == 0
    }

    override def copy(): AccumulatorV2[Long, Long] = {
      new TimestampAccumulator(timestamp)
    }

    override def reset(): Unit = {
      timestamp = 0L
    }

    override def add(v: Long): Unit = {
      timestamp = timestamp.max(v)
    }

    override def merge(other: AccumulatorV2[Long, Long]): Unit = {
      timestamp = timestamp.max(other.value)

      // Since for every write/scan operation, each executor holds its own copy of
      // client. We need to update the propagated timestamp on the driver based on
      // the latest propagated timestamp from all executors through TimestampAccumulator.
      syncClient.updateLastPropagatedTimestamp(timestampAccumulator.value)
    }

    override def value: Long = timestamp
  }

  private val timestampAccumulator = new TimestampAccumulator()
  sc.register(timestampAccumulator)


  def write(records: RDD[Record], offsetRanges: Array[OffsetRange]): Unit = {
    // Get the client's last propagated timestamp on the driver.
    val lastPropagatedTimestamp = syncClient.getLastPropagatedTimestamp

    records.foreachPartition(iterator => {
      val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
      (o.untilOffset-o.fromOffset) > 0 match {
        case true =>
          info(s"Processing ${o.topic}-${o.partition}: [${o.fromOffset} to ${o.untilOffset}] ...",LogType.Message,MonitorType.Functional)

          val pendingErrors = writePartitionRows(iterator, lastPropagatedTimestamp)

          val rowErrors = pendingErrors.getRowErrors

          if (rowErrors.nonEmpty) {
            val runtimeErrors = rowErrors.filter(error => !error.getErrorStatus.isAlreadyPresent && !error.getErrorStatus.isNotFound)
            runtimeErrors.nonEmpty match {
              case true =>
                val errors = runtimeErrors.take(10).map(_.toString).mkString(System.lineSeparator)
                error(s"Failed to write ${rowErrors.length} records to Kudu; sample errors: ${System.lineSeparator}${errors}")
                throw new RuntimeException(s"Failed to write ${rowErrors.length} records to Kudu")
              case _ =>
                val errors = rowErrors.take(10).map(_.toString).mkString(System.lineSeparator)
                warn(s"Failed to write ${rowErrors.length} records to Kudu; sample errors: ${System.lineSeparator}${errors}",LogType.Warning,MonitorType.Functional)
            }
          }

          info(s"Done ${o.topic}-${o.partition}!!",LogType.Message,MonitorType.Functional)
       case _ =>
      }

    })
  }

  private def writePartitionRows(iterator: Iterator[Record], lastPropagatedTimestamp: Long): RowErrorsAndOverflowStatus = {

    // Since each executor has its own KuduClient, update executor's propagated timestamp
    // based on the last one on the driver.
    syncClient.updateLastPropagatedTimestamp(lastPropagatedTimestamp)

    val session: KuduSession = syncClient.newSession
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    session.setIgnoreAllDuplicateRows(false)

    val timestampExtractor = new TimestampExtractor

    var tables: Map[String, Option[KuduTable]] = Map()
    var mappings: Map[(String, Operation), Map[String, String]] = Map()

    for (record <- iterator) {

      var columnName = ""

      try {

        debug(s"Writing ${record.id} -> ${record.table} ${record.operation} ${record.data}")

        val tableOption = tables.get(record.table) match {
          case Some(table) => table
          case _ =>
            val newTable = getKuduTable(record)
            tables = tables + (record.table -> newTable)
            newTable
        }

        tableOption match {
          case Some(table) =>

            val data = record.data

            val operation = record.operation match {
              case UPSERT => table.newUpsert()
              case INSERT => table.newInsert()
              case UPDATE => table.newUpdate()
              case DELETE => table.newDelete()
            }

            val columnsMappings = mappings.get(record.table, record.operation) match {
              case Some(columnsMappings) => columnsMappings
              case _ =>
                val columnsMappings = normalizeMapKeys(record.operation, data.getKeys, table.getSchema)
                mappings = mappings + ((record.table, record.operation) -> columnsMappings)
                columnsMappings
            }

            for((name, dataKey) <- columnsMappings.toIterator) {

              columnName = name

              val c: ColumnSchema = table.getSchema.getColumn(name)
              data.isNullAt(dataKey) match {
                case true =>
                  c.isKey || !c.isNullable match {
                    case true => throw DataException("Column cannot be set to null")
                    case _ => operation.getRow.setNull(name)
                  }
                case false =>
                  c.getType match {
                    case Type.STRING => operation.getRow.addString(name, data.getString(dataKey))
                    case Type.INT8 => operation.getRow.addByte(name, data.getByte(dataKey))
                    case Type.INT16 => operation.getRow.addShort(name, data.getShort(dataKey))
                    case Type.INT32 => operation.getRow.addInt(name, data.getInt(dataKey))
                    case Type.INT64 => operation.getRow.addLong(name, data.getLong(dataKey))
                    case Type.DOUBLE => operation.getRow.addDouble(name, data.getDouble(dataKey))
                    case Type.BOOL => operation.getRow.addBoolean(name, data.getBoolean(dataKey))
                    case Type.UNIXTIME_MICROS => operation.getRow.addLong(name, timestampExtractor.getTimestamp(data.getStringOrLong(dataKey)))
                    case _ => throw DataException("Type not implemented: " + c.getType)
                  }
              }
            }

            session.apply(operation)

          case _ =>
        }

      } catch {
        case e: DataException => error(s"${record.id} (${new Date(record.timestamp)}) -> " + s"${record.table} ${record.operation} ${record.data} (${columnName}): " + e.getMessage)
      }

    }

    session.close()

    // Update timestampAccumulator with the client's last propagated
    // timestamp on each executor.
    timestampAccumulator.add(syncClient.getLastPropagatedTimestamp)

    session.getPendingErrors
  }

  private def getKuduTable(record: Record): Option[KuduTable] = {
    val default_name = "impala::" + record.table
    syncClient.tableExists(default_name) match {
      case true =>
        info(s"Getting NEW table schema: ${default_name}",LogType.Message,MonitorType.Functional)
        Option(syncClient.openTable(default_name))
      case _ =>
        syncClient.tableExists(record.table) match {
          case true =>
            info(s"Getting NEW table schema: ${record.table}",LogType.Message,MonitorType.Functional)
            Option(syncClient.openTable(record.table))
          case _ =>
            error(s"Table does NOT exist: ${record.table}")
            Option.empty
        }
    }
  }

}

private object KuduStreamWriter extends FunctionalLogger {

  /**
    * Returns a new Kerberos-authenticated [[Subject]] if the Spark context contains
    * principal and keytab options, otherwise returns the currently active subject.
    *
    * The keytab and principal options should be set when deploying a Spark
    * application in cluster mode with Yarn against a secure Kudu cluster. Spark
    * internally will grab HDFS and HBase delegation tokens (see
    * [[org.apache.spark.deploy.SparkSubmit]]), so we do something similar.
    *
    * This method can only be called on the driver, where the SparkContext is
    * available.
    *
    * @return A Kerberos-authenticated subject if the Spark context contains
    *         principal and keytab options, otherwise returns the currently
    *         active subject
    */
  private def getSubject(sc: SparkContext): Subject = {
    val subject = Subject.getSubject(AccessController.getContext)

    val principal = sc.getConf.getOption("spark.yarn.principal").getOrElse(return subject)
    val keytab = sc.getConf.getOption("spark.yarn.keytab").getOrElse(return subject)

    info(s"Logging in as principal $principal with keytab $keytab",LogType.Message,MonitorType.Functional)

    val conf = new Configuration {
      override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = {
        val options = Map(
          "principal" -> principal,
          "keyTab" -> keytab,
          "useKeyTab" -> "true",
          "useTicketCache" -> "false",
          "doNotPrompt" -> "true",
          "refreshKrb5Config" -> "true"
        )

        Array(new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
          options.asJava))
      }
    }

    val loginContext = new LoginContext("kudu-spark", new Subject(), null, conf)
    loginContext.login()
    loginContext.getSubject
  }
}

private object KuduClientCache {
  private val asyncCache = new mutable.HashMap[String, AsyncKuduClient]()

  /**
    * Set to
    * [[org.apache.spark.util.ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY]].
    * The client instances are closed through the JVM shutdown hook
    * mechanism in order to make sure that any unflushed writes are cleaned up
    * properly. Spark has no shutdown notifications.
    */
  private val ShutdownHookPriority = 100

  def getAsyncClient(kuduMaster: String): AsyncKuduClient = {
    asyncCache.synchronized {
      if (!asyncCache.contains(kuduMaster)) {
        val asyncClient = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMaster).build()
        ShutdownHookManager.get().addShutdownHook(
          new Runnable {
            override def run(): Unit = asyncClient.close()
          }, ShutdownHookPriority)
        asyncCache.put(kuduMaster, asyncClient)
      }
      return asyncCache(kuduMaster)
    }
  }
}
