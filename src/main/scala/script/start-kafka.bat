@echo off


set PROJECT_ROOT=%~dp0..
set DATA_DIR=%PROJECT_ROOT%\kafka-data


set KAFKA_HOME=C:\kafka_2.12-2.4.0


if not exist "%DATA_DIR%" (
    mkdir "%DATA_DIR%"
)


start "Zookeeper" cmd /k ^
"%KAFKA_HOME%\bin\windows\zookeeper-server-start.bat ^
%KAFKA_HOME%\config\zookeeper.properties"


timeout /t 5 > nul


start "Kafka" cmd /k ^
"%KAFKA_HOME%\bin\windows\kafka-server-start.bat ^
%KAFKA_HOME%\config\server.properties ^
--override log.dirs=%DATA_DIR%"