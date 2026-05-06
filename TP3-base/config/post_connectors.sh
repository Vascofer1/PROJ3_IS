#!/bin/bash

curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://connect:8083/connectors -d @source-authors.json
curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://connect:8083/connectors -d @source-books.json
curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://connect:8083/connectors -d @sink.json
curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://connect:8083/connectors -d @sink-total-statistics.json
curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://connect:8083/connectors -d @sink-window-statistics.json