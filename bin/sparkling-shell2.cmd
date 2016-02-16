@echo off

rem Top-level directory for this product
set TOPDIR=%~dp0..
call %TOPDIR%\bin\sparkling-env.cmd
rem Verify there is Spark installation
call :checkSparkHome


rem end of checking Sparkling environment

rem Default memory for shell
set DRIVER_MEMORY=3G
if not defined DRIVER_MEMORY (
SET DRIVER_MEMORY=2G
)
rem Default MASTER
if not defined MASTER (
SET MASTER=local-cluster[3,2,2048]
)

call :banner

cd %TOPDIR%
%SPARK_HOME%/bin/spark-shell.cmd --jars %TOPDIR%/assembly/build/libs/%FAT_JAR% --driver-memory %DRIVER_MEMORY% --conf spark.driver.extraJavaOptions="-XX:MaxPermSize=384m" %*

exit /b %ERRORLEVEL%
rem end of main script

rem define functions
:checkSparkHome
rem Example class prefix
if not exist "%SPARK_HOME%\" (
   echo Please setup SPARK_HOME variable to your Spark installation!
   exit /b -1
)
exit /b 0

:banner
echo[
echo -----
echo   Spark master (MASTER)     : %MASTER%
echo   Spark home   (SPARK_HOME) : %SPARK_HOME%
echo   H2O build version         : %H2O_VERSION%.%H2O_BUILD% (%H2O_NAME%)
echo   Spark build version       : %SPARK_VERSION%
echo ----
echo[
exit /b 0
