#!/bin/bash

# INPUT VARS

JAR=$1
CLASS=$2
NAME=$3
CONFIG_PATH=$4
NAME_LOG_FILE=$5
USER=$6
SPARK_CONF_FILE=$7

KEYTAB=${HOME}/${USER}.keytab
KEYTAB_KAFKA=${HOME}/${USER}_kafka.keytab
PRINCIPAL=${USER}@DOMAIN


loadJars(){
    echo "--jars "$(ls ../lib/*.jar | tr '\n' ',')
}

loadConf(){
   while read -r line
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
  --name ${NAME} \
  --class ${CLASS} \
$(loadConf) \
$(loadKerberosConf) \
$(loadJars) \
  --files ${CONFIG_PATH}/application.conf#application.conf,${CONFIG_PATH}/kafka_jaas_ex.conf#kafka_jaas_ex.conf,${CONFIG_PATH}/log4j-${NAME}.xml#log4j-${NAME}.xml,${KEYTAB_KAFKA}#${USER}_kafka.keytab \
  --driver-java-options="-Dlog4j.configuration=file:./$NAME_LOG_FILE -Djava.security.auth.login.config=file:./kafka_jaas_ex.conf" \
  --conf spark.driver.extraJavaOptions="-Djava.security.auth.login.config=kafka_jaas_ex.conf -Dlog4j.configuration=./$NAME_LOG_FILE" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=./$NAME_LOG_FILE -Djava.security.auth.login.config=./kafka_jaas_ex.conf" \
  --conf spark.history.kerberos.principal=$PRINCIPAL \
  --conf spark.history.kerberos.keytab=$KEYTAB \
  ${JAR} \
  -f "./" \
  -a ${NAME}
