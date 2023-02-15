#!/bin/sh

cd /home/runner/work/IGinX/IGinX/

mvn test -q -Dtest=TagIT -DfailIfNoTests=false

if [ $? -ne 0 ];then
	echo " make  -- Faile  : "$?
	exit 1
else
	echo " make  -- Success !"
	exit 0
fi

result = $?
echo "?????????????" $?

cd /home/runner/work/IGinX/IGinX/test

if [ result -ne 0 ];then
	echo " make  -- Faile  : "$?
	exit 1
else
	echo " make  -- Success !"
	exit 0
fi