#!/bin/sh

set -e

sh -c "mvn test -q -Dtest=TagIT -DfailIfNoTests=false"