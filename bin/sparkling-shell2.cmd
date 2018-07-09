@echo off

rem Top-level directory for this product
set TOPDIR=%~dp0..
call %TOPDIR%\bin\sparkling-env.cmd
rem Verify there is Spark installation
call %LIBSW% checkJava
call %LIBSW% checkSparkHome
call %LIBSW% checkSparkVersion
call %LIBSW% checkFatJarExists
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

call %SPARK_HOME%/bin/spark-shell2.cmd ^
 --driver-memory %DRIVER_MEMORY% ^
 --jars %FAT_JAR_FILE% ^
 %*

exit /b %ERRORLEVEL%
rem end of main script

