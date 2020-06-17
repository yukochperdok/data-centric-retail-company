#!/bin/bash

# INPUT VARS
JAR_NAME=$1
CLASS=$2
SPARK_FILE=$3
NAME=$4
CONFIG_PATH=$5
NAME_LOG_FILE=$6
USER=`whoami`
DATA_DATE=$7 #format: yyyy-mm-dd
KEYTAB=${HOME}/${USER}.keytab
PRINCIPAL=${USER}@DOMAIN


loadJars(){
    echo "--jars "$(ls ../lib/*.jar | tr '\n' ',')
}

loadPrincJar(){
    echo $(ls ../lib/${JAR_NAME}-*.jar)
}

loadConf(){
    while read -r line || [[ -n "$line" ]];
    do
       echo "  --"$line
    done < "$SPARK_FILE"
}

loadKerberosConf(){
if [ -n "$KEYTAB" ] && [ -n "$PRINCIPAL" ];
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
$(loadKerberosConf) \
$(loadConf) \
$(loadJars) \
  --files ${CONFIG_PATH}/application.conf#application.conf,${CONFIG_PATH}/${NAME_LOG_FILE}#${NAME_LOG_FILE} \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=./$NAME_LOG_FILE" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=./$NAME_LOG_FILE" \
  --conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
$(loadPrincJar) \
  "./" \
  ${USER} \
  ${DATA_DATE}
