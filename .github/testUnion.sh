#!/bin/sh

pwd

#cd ..

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

done <  ./test/src/test/java/cn/edu/tsinghua/iginx/integration/testControler/testTask.txt

#cd ./test
