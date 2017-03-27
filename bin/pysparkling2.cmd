@echo off

rem Top-level directory for this product
set TOPDIR=%~dp0..
call %TOPDIR%\bin\sparkling-env.cmd
rem Verify there is Spark installation
call %LIBSW% checkSparkHome
call %LIBSW% checkSparkVersion
rem end of checking Sparkling environment

call %LIBSW% :banner

rem setup pysparkling command line
rem Because of SPARK-18648 we need to put assembly also on driver/executor class paths
SET SPARK_OPT_JARS=--jars !FAT_JAR_FILE! --conf spark.driver.extraClassPath=!FAT_JAR_FILE! --conf spark.executor.extraClassPath=!FAT_JAR_FILE!
SET SPARK_OPT_PYFILES=--py-files !PY_EGG_FILE!
cd %TOPDIR%
SET PYTHONPATH=%PY_EGG_FILE%:%PYTHONPATH%
call !SPARK_HOME!/bin/pyspark2.cmd !SPARK_OPT_PYFILES! %SPARK_OPT_JARS% %*

exit /b %ERRORLEVEL%
rem end of main script

