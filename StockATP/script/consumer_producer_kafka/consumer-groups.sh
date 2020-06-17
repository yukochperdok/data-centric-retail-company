#!/usr/bin/env bash

export KAFKA_OPTS="-Djava.security.auth.login.config=kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf"
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group mdtin_bdi_sap_datalake-gid --command-config consumer-oms.properties
