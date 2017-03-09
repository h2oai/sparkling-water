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

SET SPARK_OPTS=--driver-memory !DRIVER_MEMORY! --conf spark.driver.extraJavaOptions="-XX:MaxPermSize=384m"
SET SPARK_OPT_JARS=--jars !TOPDIR!/assembly/build/libs/!FAT_JAR!
cd %TOPDIR%
call !SPARK_HOME!/bin/spark-shell2.cmd !SPARK_OPT_JARS! !SPARK_OPTS! %*

exit /b %ERRORLEVEL%
rem end of main script

