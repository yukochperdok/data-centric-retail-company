#!/bin/bash

export KAFKA_OPTS="-Djava.security.auth.login.config=kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf"
kafka-console-producer --broker-list localhost:9092 --topic omnichannel_DBReplication --producer.config producer-oms.properties

