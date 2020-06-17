#!/bin/bash

export KAFKA_OPTS="-Djava.security.auth.login.config=kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf"
kafka-console-consumer --bootstrap-server localhost:9092 --topic omnichannel_DBReplication --consumer.config consumer-oms.properties --from-beginning

