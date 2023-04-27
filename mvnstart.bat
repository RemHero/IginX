@echo off
if "%1" == "" (
	bash -c "mvn clean install -Dmaven.test.skip=true" -T 5
) else if "%1" == "filesystem" (
	bash -c "mvn install -am -pl dataSources/filesystem -Dmaven.test.skip=true" -T 5
) else if "%1" == "influxdb" (
	bash -c "mvn install -am -pl dataSources/influxdb -Dmaven.test.skip=true" -T 5
) else (
	bash -c "mvn install -am -pl %1 -Dmaven.test.skip=true" -T 5
)
