@echo off

rem Top-level directory for this product
set TOPDIR=%~dp0..
call %TOPDIR%\bin\sparkling-env.cmd
rem Verify there is Spark installation
call %LIBSW% checkJava
call %LIBSW% checkSparkHome
call %LIBSW% checkSparkVersion
call %LIBSW% checkPyZipExists
rem end of checking Sparkling environment

call %LIBSW% :banner

rem Setup pysparkling command line
SET PYTHONPATH=%PY_ZIP_FILE%:%PYTHONPATH%
call %SPARK_HOME%/bin/pyspark2.cmd ^
 --py-files %PY_ZIP_FILE% ^
 %*
call !SPARK_HOME!/bin/pyspark2.cmd !SPARK_OPT_PYFILES! %SPARK_OPT_JARS% %*

exit /b %ERRORLEVEL%
rem end of main script

