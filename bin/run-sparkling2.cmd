@echo off

rem Top-level directory for this product
set TOPDIR=%~dp0..
call %TOPDIR%\bin\sparkling-env.cmd
rem Verify Spark installation
call %LIBSW% checkSparkHome
call %LIBSW% checkSparkVersion
rem end of checking Sparkling environment

set DRIVER_CLASS=water.SparklingWaterDriver

if not defined MASTER (
set MASTER=%DEFAULT_MASTER%
) 

if not defined DRIVER_MEMORY (
set DRIVER_MEMORY=%DEFAULT_DRIVER_MEMORY%
) 

if not defined H2O_SYS_OPS (
set H2O_SYS_OPS=
) 

echo ---------
echo   Using master    (MASTER)       : %MASTER%
echo   Driver memory   (DRIVER_MEMORY): %DRIVER_MEMORY%
echo   H2O JVM options (H2O_SYS_OPS)  : %H2O_SYS_OPS%
echo ---------

cd %TOPDIR%
call %SPARK_HOME%/bin/spark-submit2.cmd ^
 --class %DRIVER_CLASS% ^
 --master %MASTER% ^
 --driver-memory %DRIVER_MEMORY% ^
 --driver-java-options "%H2O_SYS_OPS%" ^
 --conf spark.driver.extraJavaOptions="-XX:MaxPermSize=384m" ^
 %VERBOSE% ^
 %TOPDIR%/assembly/build/libs/%FAT_JAR% ^
 %*
exit /b %ERRORLEVEL%

rem end of main script

