#!/bin/bash

CONF_PATH=../conf
PROCESS_NAME="customerOrderReservationStreaming"
JAR_NAME="bdomnch-reservas-stock-atp-source"
SECOND_CONF_FILE="$PROCESS_NAME.conf"
CLASS="com.training.bigdata.omnichannel.customerOrderReservation.CustomerOrderReservationStreamingMain"

./spark-launcher.sh ${JAR_NAME} ${CLASS} ${PROCESS_NAME} ${CONF_PATH}


