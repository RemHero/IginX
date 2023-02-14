#!/bin/sh

cd /home/runner/work/IGinX/IGinX/

mvn test -q -Dtest=TagIT -DfailIfNoTests=false

cd /home/runner/work/IGinX/IGinX/test