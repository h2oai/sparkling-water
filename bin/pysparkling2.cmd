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
SET SPARK_OPT_PYFILES=--py-files !PY_ZIP_FILE!
SET PYTHONPATH=%PY_ZIP_FILE%:%PYTHONPATH%
call !SPARK_HOME!/bin/pyspark2.cmd !SPARK_OPT_PYFILES! %*

exit /b %ERRORLEVEL%
rem end of main script

