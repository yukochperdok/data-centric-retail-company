#!/bin/bash

#INPUT VARS
JAR_NAME=$1
CLASS=$2
PROCESS_NAME=$3
CONF_PATH=$4
USER=`whoami`
KEYTAB=${HOME}/${USER}.keytab
KEYTAB_KAFKA=${HOME}/${USER}_kafka.keytab
PRINCIPAL=${USER}@DOMAIN
NAME_LOG_FILE="log4j-$PROCESS_NAME.xml"
SPARK_CONF_FILE="../conf/$PROCESS_NAME.cfg"
SECOND_CONF_FILE="$PROCESS_NAME.conf"


loadJars(){
    echo "--jars "$(ls ../lib/*.jar | tr '\n' ',')
}

loadPrincJar(){
    echo $(ls ../lib/${JAR_NAME}-*.jar)
}

loadConf(){
   while read -r line || [[ -n "$line" ]]
do
   echo "  --"$line
done < "$SPARK_CONF_FILE"
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
    --name ${PROCESS_NAME} \
    --files ${KEYTAB_KAFKA},${CONF_PATH}/kafka_jaas_ex.conf,${CONF_PATH}/${SECOND_CONF_FILE},${CONF_PATH}/application.conf,${CONF_PATH}/${NAME_LOG_FILE} \
    --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=$NAME_LOG_FILE -Dconfig.resource=./$SECOND_CONF_FILE -Djava.security.auth.login.config=./kafka_jaas_ex.conf" \
    --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=$NAME_LOG_FILE -Djava.security.auth.login.config=./kafka_jaas_ex.conf" \
    --conf spark.history.kerberos.principal=$PRINCIPAL \
    --conf spark.history.kerberos.keytab=$KEYTAB \
    $(loadKerberosConf) \
    $(loadConf) \
    --class ${CLASS} \
    $(loadJars) \
    $(loadPrincJar) \
    ${USER}