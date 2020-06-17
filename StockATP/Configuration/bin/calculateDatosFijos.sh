#!/bin/bash

# INPUT VARS
DATA_DATE=$1 #format: yyyy-mm-dd

JAR_NAME="bdomnch-stock-atp"
CLASS="com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.DatosFijosMain"
SPARK_FILE="../conf/calculateDatosFijos.cfg"
NAME="calculateDatosFijos"
CONFIG_PATH="../conf/"
NAME_LOG_FILE="log4j-calculateDatosFijos.xml"

./spark-launcher-cluster.sh ${JAR_NAME} ${CLASS} ${SPARK_FILE} ${NAME} ${CONFIG_PATH} ${NAME_LOG_FILE} ${DATA_DATE}