@echo off

rem Top-level directory for this product
set TOPDIR=%~dp0..
call %TOPDIR%\bin\sparkling-env.cmd
rem Verify there is Spark installation
call :checkSparkHome
call :checkSparkVersion
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
:checkSparkVersion
for /f "delims=" %%i in ( 'CMD /C %SPARK_HOME%/bin/spark-submit.cmd --version 2^>^&1 1^>NUL ^| find "version" ') do set linewithversion=%%i
set INSTALLED_SPARK_VERSION=%linewithversion:~-5%

if NOT "%INSTALLED_SPARK_VERSION%"=="%SPARK_VERSION%" (
   echo You are trying to use Sparkling Water built for Spark %SPARK_VERSION%, but your %%SPARK_HOME(=%SPARK_HOME%^) property points to Spark of version %INSTALLED_SPARK_VERSION%. Please ensure correct Spark is provided and re-run Sparkling Water.
   call :haltHelper 2> nul
	)
exit /b 0

:checkSparkHome
rem Example class prefix
if not exist "%SPARK_HOME%/bin/spark-submit.cmd" (
   echo Please setup SPARK_HOME variable to your Spark installation!
   call :haltHelper 2> nul
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

:haltHelper
() 
exit /b 1


