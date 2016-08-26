@echo off

if not defined TOPDIR (
    echo Caller has to setup TOPDIR variable!
    exit /b -1
)

rem Version of this distribution                             
for /f "tokens=2 delims==" %%i in ('findstr /r "^version=.*$" %TOPDIR%\gradle.properties') do (@set VERSION=%%i)
for /f "tokens=2 delims==" %%i in ('findstr /r "^h2oMajorVersion=.*$" %TOPDIR%\gradle.properties') do (@set H2O_VERSION=%%i)
for /f "tokens=2 delims==" %%i in ('findstr /r "^h2oBuild=.*$" %TOPDIR%\gradle.properties') do (@set H2O_BUILD=%%i)
for /f "tokens=2 delims==" %%i in ('findstr /r "^h2oMajorName=.*$" %TOPDIR%\gradle.properties') do (@set H2O_NAME=%%i)
for /f "tokens=2 delims==" %%i in ('findstr /r "^sparkVersion=.*$" %TOPDIR%\gradle.properties') do (@set SPARK_VERSION=%%i)
for /f "tokens=2 delims==" %%i in ('findstr /r "^sparkVersion=.*$" %TOPDIR%\gradle.properties') do (@set SCALA_PARSED_VERSION=%%i)

rem Ensure that scala version contains only major version
for /f "tokens=1,2 delims=." %%j in ("%SCALA_PARSED_VERSION%") do (@set SCALA_VERSION=%%j.%%k)

rem Fat jar for this distribution
set FAT_JAR=sparkling-water-assembly_%SCALA_VERSION%-%VERSION%-all.jar
set FAT_JAR_FILE=%TOPDIR%\assembly\build\libs\%FAT_JAR%

rem Setup loging and output

set tmpdir=%TMP%
set SPARK_LOG_DIR=%tmpdir%\spark\logs
set SPARK_WORKER_DIR=%tmpdir%\spark\work
set SPARK_LOCAL_DIRS=%tmpdir%\spark\work
