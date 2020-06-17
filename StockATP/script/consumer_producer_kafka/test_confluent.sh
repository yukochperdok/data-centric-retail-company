#!/bin/bash

if [ $# -lt 1 ]
then
    echo "Usage: ./test_confluent.sh <<events.json>>"
	echo "Usage: For example: ./test_confluent.sh 3b_location_capacity_oms.json"
	exit 1
else
	curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" -d"$(< $1)" http://localhost:8082/topics/omnichannel_DBReplication
fi

