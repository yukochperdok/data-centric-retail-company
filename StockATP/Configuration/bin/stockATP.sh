#!/bin/bash

# INPUT VARS
DATA_DATE=$1  #format: yyyy-mm-dd

JAR_NAME="bdomnch-stock-atp"
CLASS="com.training.bigdata.omnichannel.stockATP.calculateStockATP.StockATPMain"
SPARK_FILE="../conf/stockATP.cfg"
NAME="stockATP"
CONFIG_PATH="../conf/"
NAME_LOG_FILE="log4j-stockATP.xml"

./spark-launcher-cluster.sh ${JAR_NAME} ${CLASS} ${SPARK_FILE} ${NAME} ${CONFIG_PATH} ${NAME_LOG_FILE} ${DATA_DATE}
