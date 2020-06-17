#!/bin/bash

#OPTIONAL INPUT VARS
FILE=$1
USER=`whoami`

# LOCAL VARS
CLASS="com.training.bigdata.omnichannel.customerOrderReservation.producer.KafkaAvroProducer"
JAR_NAME="bdomnch-reservas-stock-atp-source"
LIB_PATH="../lib"
CONF_PATH="../conf"
CONF_FILE="${CONF_PATH}/customerOrderReservationStreaming.conf"
APP_CONF_FILE="${CONF_PATH}/application.conf"
JAAS_FILE="${CONF_PATH}/kafka_jaas_producer.conf"
NAME_LOG_FILE="file:${CONF_PATH}/log4j-producer.xml"
SCALA_LIBRARY="/opt/cloudera/parcels/SPARK2/lib/spark2/jars/scala-*.jar"
SPARK_LIBRARY="/opt/cloudera/parcels/SPARK2/lib/spark2/jars/spark-*.jar"

loadClasspath(){
    echo "-cp "${LIB_PATH}/${JAR_NAME}-*.jar':'${LIB_PATH}/*':'$(ls ${SCALA_LIBRARY} | tr '\n' ':')$(ls ${SPARK_LIBRARY} | tr '\n' ':')
}

java -Dlog4j.debug -Dlog4j.configuration=$NAME_LOG_FILE \
    $(loadClasspath) \
    ${CLASS} \
    ${CONF_FILE} \
    ${APP_CONF_FILE} \
    ${JAAS_FILE} \
    ${USER} \
    ${FILE}