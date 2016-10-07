@echo off

if not defined TOPDIR (
    echo Caller has to setup TOPDIR variable!
    exit /b -1
)


rem Version of this distribution
for /f "tokens=2 delims==" %%i in ('TYPE %TOPDIR%\gradle.properties ^| findstr /r "^version="') do (@set VERSION=%%i)
for /f "tokens=2 delims==" %%i in ('TYPE %TOPDIR%\gradle.properties ^| findstr /r "^h2oMajorVersion="') do (@set H2O_VERSION=%%i)
for /f "tokens=2 delims==" %%i in ('TYPE %TOPDIR%\gradle.properties ^| findstr /r "^h2oBuild="') do (@set H2O_BUILD=%%i)
for /f "tokens=2 delims==" %%i in ('TYPE %TOPDIR%\gradle.properties ^| findstr /r "^h2oMajorName="') do (@set H2O_NAME=%%i)
for /f "tokens=2 delims==" %%i in ('TYPE %TOPDIR%\gradle.properties ^| findstr /r "^sparkVersion="') do (@set SPARK_VERSION=%%i)

rem Fat jar for this distribution
set FAT_JAR=sparkling-water-assembly-%VERSION%-all.jar
set FAT_JAR_FILE=%TOPDIR%\assembly\build\libs\%FAT_JAR%

rem Setup loging and outputs

set tmpdir=%TMP%
set SPARK_LOG_DIR=%tmpdir%\spark\logs
set SPARK_WORKER_DIR=%tmpdir%\spark\work
set SPARK_LOCAL_DIRS=%tmpdir%\spark\work
