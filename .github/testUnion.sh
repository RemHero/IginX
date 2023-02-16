#!/bin/sh

pwd

cd ..

sleep 5

while read line
do
  echo "test IT name : "$line
  mvn test -q -Dtest=$line -DfailIfNoTests=false

  if [ $? -ne 0 ];then
    echo " test  -- Faile  : "$?
    exit 1
  else
    echo " test  -- Success !"
  fi

  sleep 5

done <  ./test/src/test/java/cn/edu/tsinghua/iginx/integration/testControler/testTask.txt

cd ./test
