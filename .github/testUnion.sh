#!/bin/sh

cd /home/runner/work/IGinX/IGinX/

mvn test -q -Dtest=TagIT -DfailIfNoTests=false

result =  $?

cd /home/runner/work/IGinX/IGinX/test

if [ result -ne 0 ];then
	echo " make  -- Faile  : "$?
	exit 1
else
	echo " make  -- Success !"
	exit 0
fi