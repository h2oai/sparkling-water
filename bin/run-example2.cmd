@echo off

rem Top-level directory for this product
set TOPDIR=%~dp0..
call %TOPDIR%\bin\sparkling-env.cmd
rem Verify there is Spark installation
call %LIBSW% checkSparkHome
call %LIBSW% checkSparkVersion

rem Example prefix
set PREFIX=org.apache.spark.examples.h2o
rem Name of default example
set DEFAULT_EXAMPLE=AirlinesWithWeatherDemo2

set CLASS=%~1
set BEGINNING=%CLASS:~0,2%
if "%~1" neq "" (
    if "%BEGINNING%" neq "--" (
        set EXAMPLE=%PREFIX%.%CLASS%
        shift
    )else (
        set EXAMPLE=%PREFIX%.%DEFAULT_EXAMPLE% 
    )
)else (
 set EXAMPLE=%PREFIX%.%DEFAULT_EXAMPLE%
)

if not defined MASTER (
set EXAMPLE_MASTER=%DEFAULT_MASTER%
) else (
set EXAMPLE_MASTER=%MASTER%
)
set EXAMPLE_DEPLOY_MODE=cluster
if not defined DEPLOY_MODE (
set EXAMPLE_DEPLOY_MODE=client
) else (
set EXAMPLE_DEPLOY_MODE=%DEPLOY_MODE%
)
if not defined DRIVER_MEMORY (
set EXAMPLE_DRIVER_MEMORY=%DEFAULT_DRIVER_MEMORY%
) else (
set EXAMPLE_DRIVER_MEMORY=%DRIVER_MEMORY%
)
if not defined H2O_SYS_OPS (
set EXAMPLE_H2O_SYS_OPS=
) else (
 set EXAMPLE_H2O_SYS_OPS=%H2O_SYS_OPS%
)

echo ---------
echo   Using example                  : %EXAMPLE%
echo   Using master    (MASTER)       : %EXAMPLE_MASTER%
echo   Deploy mode     (DEPLOY_MODE)  : %EXAMPLE_DEPLOY_MODE%
echo   Driver memory   (DRIVER_MEMORY): %EXAMPLE_DRIVER_MEMORY%
echo   H2O JVM options (H2O_SYS_OPS)  : %EXAMPLE_H2O_SYS_OPS%
echo ---------

set SPARK_PRINT_LAUNCH_COMMAND=1
set VERBOSE=--verbose

call %SPARK_HOME%/bin/spark-submit2.cmd ^
 --class %EXAMPLE% ^
 --master %EXAMPLE_MASTER% ^
 --driver-memory %EXAMPLE_DRIVER_MEMORY% ^
 --driver-java-options "%EXAMPLE_H2O_SYS_OPS%" ^
 --deploy-mode %EXAMPLE_DEPLOY_MODE% ^
 --conf spark.driver.extraJavaOptions="-XX:MaxPermSize=384m" ^
 %VERBOSE% ^
 %FAT_JAR_FILE% ^
 %*
exit /b %ERRORLEVEL%
rem end of main script

