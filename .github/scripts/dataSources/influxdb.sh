#!/bin/sh

for port in "$@"
do
  set -e

  if test -e "influxdb2-2.0.7-linux-amd64.tar.gz"; then
    echo "File already exists, skipping download."
  else
    sh -c "wget https://dl.influxdata.com/influxdb/releases/influxdb2-2.0.7-linux-amd64.tar.gz"
    sh -c "tar -zxf influxdb2-2.0.7-linux-amd64.tar.gz"
    sh -c "ls influxdb2-2.0.7-linux-amd64"

    sudo sh -c "cd influxdb2-2.0.7-linux-amd64/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt --engine-path=~/.influxdbv2/engine --http-bind-address=:8086 --query-memory-bytes=20971520 &"
    sh -c "sleep 30"
    sh -c "./influxdb2-2.0.7-linux-amd64/influx setup --org testOrg --bucket testBucket --username user --password 12345678 --token testToken --force"
  fi
  sh -c "sudo cp -r influxdb2-2.0.7-linux-amd64/ influxdb2-2.0.7-linux-amd64-2-$port/"
  sudo sh -c "cd influxdb2-2.0.7-linux-amd64-2-$port/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt --engine-path=~/.influxdbv2/engine --http-bind-address=:$port --query-memory-bytes=20971520 &"


done
sed -i "s/your-token/testToken/g" conf/config.properties
sed -i "s/your-organization/testOrg/g" conf/config.properties
sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties
sed -i "s/#storageEngineList=127.0.0.1#8086/storageEngineList=127.0.0.1#8086/g" conf/config.properties
sed -i "s/enablePushDown=true/enablePushDown=false/g" conf/config.properties
