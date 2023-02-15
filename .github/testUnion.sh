#!/bin/sh

cd /home/runner/work/IGinX/IGinX/

while read line
do
  echo $line

  mvn test -q -Dtest=$line -DfailIfNoTests=false

  if [ $? -ne 0 ];then
  	echo " test  -- Faile  : "$?
  	exit 1
  else
  	echo " test  -- Success !"
  fi
done <  /home/runner/work/IGinX/IGinX/test/src/test/java/cn/edu/tsinghua/iginx/integration/testControler/testTask.txt

cd /home/runner/work/IGinX/IGinX/test
