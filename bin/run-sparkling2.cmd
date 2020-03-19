@echo off

rem Top-level directory for this product
set TOPDIR=%~dp0..
call %TOPDIR%\bin\sparkling-env.cmd
rem Verify Spark installation
call %LIBSW% checkJava
call %LIBSW% checkSparkHome
call %LIBSW% checkSparkVersion
call %LIBSW% checkFatJarExists
rem end of checking Sparkling environment

set DRIVER_CLASS=ai.h2o.sparkling.SparklingWaterDriver

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

call %SPARK_HOME%/bin/spark-submit2.cmd ^
 --class %DRIVER_CLASS% ^
 --master %MASTER% ^
 --driver-memory %DRIVER_MEMORY% ^
 --driver-java-options "%H2O_SYS_OPS%" ^
 %VERBOSE% ^
 %FAT_JAR_FILE% ^
 %*
exit /b %ERRORLEVEL%

rem end of main script

