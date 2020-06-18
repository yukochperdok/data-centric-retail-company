#!/bin/bash

# INPUT VARS
JAR_NAME=$1
CLASS=$2
SPARK_CONF_FILE=$3
NAME=$4
CONFIG_PATH=$5
NAME_LOG_FILE=$6
DATE=$7
WORKFLOW=$8
USER=`whoami`
KEYTAB=${HOME}/${USER}.keytab
PRINCIPAL=${USER}@DOMAIN

loadDate(){
    if [[ -n "$DATE" ]]
    then
        echo "-d "${DATE}
    fi
}

loadHistorify(){
    if [[ -n "$WORKFLOW" ]]
    then
        echo "-w "${WORKFLOW}
    fi
}

loadJars(){
    echo "--jars "$(ls ../lib/*.jar | tr '\n' ',')
}

loadPrincJar(){
    echo $(ls ../lib/${JAR_NAME}.jar)
}

loadConf(){
    while read -r line || [[ -n "$line" ]];
    do
       echo "  --"$line
    done < "$SPARK_CONF_FILE"
}

loadKerberosConf(){
if [[ -n "$KEYTAB" ]] && [[ -n "$PRINCIPAL" ]];
  then
    echo "  --principal $PRINCIPAL";
    echo "  --keytab $KEYTAB";
fi
}

spark2-submit \
  --master yarn \
  --deploy-mode cluster \
  --name ${NAME} \
  --class ${CLASS} \
$(loadConf) \
$(loadKerberosConf) \
$(loadJars) \
  --files ${CONFIG_PATH}/application.conf,${CONFIG_PATH}/${NAME_LOG_FILE} \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=./$NAME_LOG_FILE" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=$NAME_LOG_FILE" \
  --conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
$(loadPrincJar) \
  --user ${USER} \
  --config-file-path "./" \
$(loadDate) \
$(loadHistorify)