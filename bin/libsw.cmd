call:%*
exit /b

:banner
echo[
echo -----
echo   Spark master (MASTER)     : %MASTER%
echo   Spark home   (SPARK_HOME) : %SPARK_HOME%
echo   H2O build version         : %H2O_VERSION%.%H2O_BUILD% (%H2O_NAME%)
echo   Spark build version       : %SPARK_VERSION%
echo   Scala version             : %SCALA_VERSION%
echo ----
echo[
exit /b 0

:checkSparkHome
rem Example class prefix
if not exist "%SPARK_HOME%/bin/spark-submit.cmd" (
   echo Please setup SPARK_HOME variable to your Spark installation!
   call :haltHelper 2> nul
)
exit /b 0

:checkSparkVersion
for /f "delims=" %%i in ( 'CMD /C %SPARK_HOME%/bin/spark-submit.cmd --version 2^>^&1 1^>NUL ^| findstr /v "Scala" ^| findstr "version" ') do set linewithversion=%%i
set INSTALLED_SPARK_VERSION=%linewithversion:~-5%

if NOT "%INSTALLED_SPARK_VERSION%"=="%SPARK_VERSION%" (
   echo You are trying to use Sparkling Water built for Spark %SPARK_VERSION%, but your %%SPARK_HOME(=%SPARK_HOME%^) property points to Spark of version %INSTALLED_SPARK_VERSION%. Please ensure correct Spark is provided and re-run Sparkling Water.
   call :haltHelper 2> nul
	)
exit /b 0

:haltHelper
() 
exit /b 1


