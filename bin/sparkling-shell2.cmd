@echo off

rem Top-level directory for this product
set TOPDIR=%~dp0..
call %TOPDIR%\bin\sparkling-env.cmd
rem Verify there is Spark installation
call %LIBSW% checkSparkHome
call %LIBSW% checkSparkVersion
rem end of checking Sparkling environment

rem Default memory for shell
set DRIVER_MEMORY=3G
if not defined DRIVER_MEMORY (
SET DRIVER_MEMORY=%DEFAULT_DRIVER_MEMORY%
)
rem Default MASTER
if not defined MASTER (
SET MASTER=%DEFAULT_MASTER%
)

call %LIBSW% :banner

SET SW_FAT_JAR_FILE=!TOPDIR!/assembly/build/libs/!FAT_JAR!
SET SPARK_OPTS=--driver-memory !DRIVER_MEMORY! --conf spark.driver.extraJavaOptions="-XX:MaxPermSize=384m"
rem Because of SPARK-18648 we need to put assembly also on driver/executor class paths
SET SPARK_OPT_JARS=--jars !SW_FAT_JAR_FILE! --conf spark.driver.extraClassPath=!SW_FAT_JAR_FILE! --conf spark.executor.extraClassPath=!SW_FAT_JAR_FILE!
cd %TOPDIR%
call !SPARK_HOME!/bin/spark-shell2.cmd !SPARK_OPT_JARS! !SPARK_OPTS! %*

exit /b %ERRORLEVEL%
rem end of main script

