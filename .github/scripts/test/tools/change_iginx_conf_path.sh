#!/bin/sh

set -e

#cp -f core/target/iginx-core-*-SNAPSHOT/sbin/start_iginx.sh core/target/iginx-core-*-SNAPSHOT/sbin/start_iginx.sh.bak

pwd

sed -i "s#/conf/config.properties#$1#g" core/target/iginx-core-*-SNAPSHOT/sbin/start_iginx.sh
