#!/bin/sh

pwd

cd ..

FILE = cat ./test/src/test/java/cn/edu/tsinghua/iginx/integration/testControler/testTask.txt
LISTS=($FILE)
echo "test IT name list : "${LISTS[*]}

#for line in ${LISTS[@]}
#do
#   echo "test IT name : "$line
#   mvn test -q -Dtest=$line -DfailIfNoTests=false
#
#   if [ $? -ne 0 ];then
#     echo " test  -- Faile  : "$?
#     exit 1
#   else
#     echo " test  -- Success !"
#   fi
#done

cd test