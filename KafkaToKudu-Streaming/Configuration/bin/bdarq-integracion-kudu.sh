#!/bin/bash

# INPUT VARS
USER=$1
MODE=$2

if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    exit 1
  else
    NAME="${project.artifactId}"
    JAR="../lib/${project.artifactId}-source-${project.version}.jar"
    CLASS="com.training.bigdata.arquitectura.ingestion.datalake.KafkaToKudu"
    CONFIG_PATH="../conf"
    SPARK_CONF_FILE=$CONFIG_PATH/sparkConf.cfg
    NAME_LOG_FILE="log4j-${project.artifactId}.xml"

   ./spark-launcher-${MODE}.sh ${JAR} ${CLASS} ${NAME} ${CONFIG_PATH} ${NAME_LOG_FILE} ${USER} ${SPARK_CONF_FILE}

fi