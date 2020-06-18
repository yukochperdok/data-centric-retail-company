#!/bin/bash

# INPUT VARS
DATE=$1
WORKFLOW=$2

# LOCAL VARS
NAME="INEFilesIngestion"
JAR_NAME="${project.artifactId}-source-${project.version}"
CLASS="com.training.bigdata.mdata.INEaddresses.INEFilesIngestionMain"
CONFIG_PATH="../conf"
SPARK_FILE="${CONFIG_PATH}/${NAME}.cfg"
NAME_LOG_FILE="log4j-${NAME}.xml"

# GET FLAG REPROCESS
if [[ -z ${WORKFLOW} ]]; then
  echo "Normal execution, no historic flag detected"
  WORKFLOW="normal"
elif [[ ${WORKFLOW} == "stretches" ]] || [[ ${WORKFLOW} == "types" ]];then
  echo "Historic flag detected: ${WORKFLOW} will be historified"
else
  echo "Argument not valid ${WORKFLOW}"
  exit 1
fi

./spark-launcher-cluster.sh ${JAR_NAME} ${CLASS} ${SPARK_FILE} ${NAME} ${CONFIG_PATH} ${NAME_LOG_FILE} ${DATE} ${WORKFLOW}