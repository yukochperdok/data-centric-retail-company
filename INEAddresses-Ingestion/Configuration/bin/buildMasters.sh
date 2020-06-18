#!/bin/bash

NAME="buildMasters"
JAR_NAME="${project.artifactId}-source-${project.version}"
CLASS="com.training.bigdata.mdata.INEaddresses.MastersMain"
CONFIG_PATH="../conf"
SPARK_FILE="${CONFIG_PATH}/${NAME}.cfg"
NAME_LOG_FILE="log4j-${NAME}.xml"

./spark-launcher-cluster.sh ${JAR_NAME} ${CLASS} ${SPARK_FILE} ${NAME} ${CONFIG_PATH} ${NAME_LOG_FILE}