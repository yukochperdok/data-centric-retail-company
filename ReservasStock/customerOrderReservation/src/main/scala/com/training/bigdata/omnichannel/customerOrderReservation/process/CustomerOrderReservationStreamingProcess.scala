package com.training.bigdata.omnichannel.customerOrderReservation.process

import com.training.bigdata.omnichannel.customerOrderReservation.consumer.KafkaStreamReader
import com.training.bigdata.omnichannel.customerOrderReservation.io.KuduDataSource.initCreateViews
import com.training.bigdata.omnichannel.customerOrderReservation.entities.{ArticleTranscod, ReservationActions, SiteTranscod}
import com.training.bigdata.omnichannel.customerOrderReservation.entities.EcommerceOrder._
import com.training.bigdata.omnichannel.customerOrderReservation.entities.CustomerOrdersReservations.{pkFields, toDBFields}
import com.training.bigdata.omnichannel.customerOrderReservation.events.avro.AvroInterpreter._
import com.training.bigdata.omnichannel.customerOrderReservation.io.{BroadcastWrapper, KuduDataSource}
import com.training.bigdata.omnichannel.customerOrderReservation.utils.OperationsUtils._
import com.training.bigdata.omnichannel.customerOrderReservation.utils.DateUtils.castTimestampColumn
import com.training.bigdata.omnichannel.customerOrderReservation.utils.AuditFieldsUtils.addAuditFields
import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger
import com.training.bigdata.omnichannel.customerOrderReservation.utils._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions.col

object CustomerOrderReservationStreamingProcess
  extends Serializable
    with SparkCommons
    with FunctionalLogger {

  def apply(props: PropertiesUtil): Unit = {

    implicit val sparkSession: SparkSession = createSparkSession("yarn")
    implicit val propertiesUtil: PropertiesUtil = props
    implicit val ssc: StreamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(propertiesUtil.getBatchDuration))
    implicit val kuduContext: KuduContext = new KuduContext(propertiesUtil.getKuduHosts, sparkSession.sparkContext)

    processPipeline

    setupGracefulStop(propertiesUtil.getGracefulStopHDFSDir)
  }

  def processPipeline(implicit sparkSession: SparkSession,
                               ssc: StreamingContext,
                               kuduContext: KuduContext,
                               propertiesUtil: PropertiesUtil): Unit = {

    implicit val kuduDataSource: KuduDataSource = new KuduDataSource

    val dStream: InputDStream[ConsumerRecord[String, GenericRecord]] = KafkaStreamReader.readStream(
      propertiesUtil.getReservationsTopic,
      propertiesUtil.getKafkaServers,
      propertiesUtil.getSchemaRegistryURL,
      propertiesUtil.getReservationsGroupId,
      "earliest",
      propertiesUtil.isKafkaSecured)(ssc)

    dStream.foreachRDD { rdd =>

      if (isOK) {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        initCreateViews

        val (articleTranscodMap: Broadcast[Map[String, String]],
        siteTranscodMap: Broadcast[Map[String, String]],
        reservationsMap: Broadcast[Map[(String, String), (String, String)]]
          ) =
          BroadcastWrapper.getTablesBroadcastAndUpdateThemIfExpired(
            kuduDataSource,
            ArticleTranscod.ARTICLE_TRANSCOD_VIEW,
            SiteTranscod.SITE_TRANSCOD_VIEW,
            ReservationActions.RESERVATION_ACTIONS_VIEW,
            propertiesUtil.getRefreshTranscodTablesMillis)

        processRDD(rdd, articleTranscodMap, siteTranscodMap, reservationsMap)

        dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    sys.ShutdownHookThread {
      logInfo("Shutdown hook called ...")
      ssc.stop(stopSparkContext = true, stopGracefully = true)
      logInfo("Application stopped!")
    }

    ssc.start

    logInfo("StreamingContext started ...")
  }

  private def processRDD(rdd: RDD[ConsumerRecord[String, GenericRecord]],
                         articleTranscodMap: Broadcast[Map[String, String]],
                         siteTranscodMap: Broadcast[Map[String, String]],
                         reservationsMap: Broadcast[Map[(String, String), (String, String)]])
                        (implicit sparkSession: SparkSession,
                                  kuduContext: KuduContext,
                                  kuduDataSource: KuduDataSource,
                                  propertiesUtil: PropertiesUtil): Unit = {

    logDebug(s"Num Partitions in RDD: ${rdd.getNumPartitions}")
    logDebug(s"Count per partitions in RDD: ${rdd.count}")

    val schema: Schema = getAvroSchema(propertiesUtil.getSchemaRegistryURL, propertiesUtil.getReservationsTopic)

    val ordersInputDF = parseAvroToDF(rdd, schema)
    val ordersWithCommerceItemsExploded = explodeColumnsAndSelectFields(ordersInputDF, commerceItems, commerceItemsFields, simpleFields)
    val shippingGroupsExploded = explodeColumnsAndSelectFields(ordersInputDF, shippingGroups, shippingGroupsFields, List(omsOrderId, siteId))

    val ordersExplodedDF = ordersWithCommerceItemsExploded
      .join(shippingGroupsExploded, Seq(shippingGroupId, omsOrderId, siteId))
      .transform(
        addArticleBasedOnSite andThen
        filterNullsAndLog(store, Constants.Errors.RESERVATION_WITH_NULL_STORE) andThen
        filterNullsAndLog(article, Constants.Errors.RESERVATION_WITH_NULL_ARTICLE) andThen
        filterNullsAndLog(pickingDate, Constants.Errors.RESERVATION_WITH_NULL_PICKING_DATE)
      ).drop(shippingGroupId)

    val ordersFilteredMarketplaceDF = ordersExplodedDF
      .filter(!(col(orderType) isin(propertiesUtil.getOrderTypesToIgnore:_*)))
      .drop(orderType)
      .cache

    val (newReservationsDF, oldReservationsDF) = separateByAction(reservationsMap, propertiesUtil.getUpsertAction, propertiesUtil.getDeleteAction)(
      ordersFilteredMarketplaceDF
        .transform(
          transcodArticleAndSite(article, store, articleTranscodMap, siteTranscodMap)
        )
    )

    val newReservationsWithCorrectAmountDF = newReservationsDF
      .transform(
        calculateAmountAndUnitOfMeasurement andThen
        castTimestampColumn(pickingDate) andThen
        castTimestampColumn(deliveryDate) andThen
        castTimestampColumn(creationDate) andThen
        toDBFields andThen
        addAuditFields(propertiesUtil.user)
      )

    val oldReservationsToDeleteDF = oldReservationsDF
      .transform(toDBFields)
      .select(pkFields.head, pkFields.tail:_*)

    kuduDataSource.upsertToKudu(newReservationsWithCorrectAmountDF, propertiesUtil.getCustomerOrdersReservationTable)
    kuduDataSource.deleteFromKudu(oldReservationsToDeleteDF, propertiesUtil.getCustomerOrdersReservationTable)
  }
}
