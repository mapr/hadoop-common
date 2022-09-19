@echo off
setlocal enabledelayedexpansion

rem Configures hadoop 1 or 2 for windows client. It is called by configure.bat
rem The parameters are: configure-hadoop.bat {hadoop version:1|2} {resource manager hostname} {history server hostname}

if not defined MAPR_HOME (
  echo MAPR_HOME is not set. Set MAPR_HOME to the installation folder and run this command.
  goto end
)

set MY_MAPR_HOME=%MAPR_HOME%
set FC=%MY_MAPR_HOME:~0,1%

if [!FC!]==[^"] (
  set MY_MAPR_HOME=%MAPR_HOME:~1,-1%
)

rem The hadoop directory parsed to point to the given hadoop version
set RM_LIST=%2
set HS_IP=%3
set CLUSTER_NAME=%1



set HADOOP2_DIR=

rem Set hadoop variables from version file
cd %MAPR_HOME%\conf
FOR /F "delims=\= tokens=1-2 " %%i in (hadoop_version) do set %%i=%%j
cd %MAPR_HOME%\server

goto main
rem BEGIN Functions


:configureHadoop

rem Here is where all the functionality to configure hadoop 2 is
rem Copy over the jars needed for hadoop 2
copy /Y %MY_MAPR_HOME%\lib\json-1.8.jar %HADOOP2_DIR%\share\hadoop\common\lib > NUL

rem Function to configure hadoop 2 conf directory. Sets RM and HS IPs in this method

set OUTTEXTFILE=%HADOOP2_DIR%\etc\hadoop\yarn-site.xml
set TMPFILE=%HADOOP2_DIR%\etc\hadoop\yarn-site.xml.tmp

rem make backup
if exist "%OUTTEXTFILE%" copy /Y "%OUTTEXTFILE%" "%OUTTEXTFILE%-%date:~-4,4%-%date:~-7,2%-%date:~-10,2%-%time:~0,2%-%time:~3,2%-%time:~6,2%" > NUL

REM phatJar
for /f "delims=" %%F in ('dir /b /s "%HADOOP2_DIR%\share\hadoop\yarn\hadoop-yarn-common-*.jar" ^| findstr /V source 2^>nul') do set phatJar=%%F

if [%RM_LIST%] == [""] (
    call %HADOOP2_DIR%\bin\hadoop jar %phatJar% org.apache.hadoop.yarn.configuration.YarnSiteMapRHAXmlBuilder %OUTTEXTFILE% > %TMPFILE%
    REM This is done with a GOTO because if you surround the next section in (), it fails
    goto configureHS
)

echo %RM_LIST% | findstr /r "," > nul
if "%errorlevel%" == "0" (
    REM multiple rm
    call %HADOOP2_DIR%\bin\hadoop jar %phatJar% org.apache.hadoop.yarn.configuration.YarnHASiteXmlBuilder %RM_LIST% %CLUSTER_NAME% %OUTTEXTFILE% > %TMPFILE%
) else (
    REM single rm
    REM
    call %HADOOP2_DIR%\bin\hadoop jar %phatJar% org.apache.hadoop.yarn.configuration.YarnSiteXmlBuilder %RM_LIST% %OUTTEXTFILE% > %TMPFILE%
)

:configureHS

if exist "%TMPFILE%" move /Y "%TMPFILE%" "%OUTTEXTFILE%" > NUL

rem Set hs to 0.0.0.0 if not set
if "%HS_IP%" == "" (
    set HS_IP=0.0.0.0
)
setlocal DisableDelayedExpansion
set INTEXTFILE=%HADOOP2_DIR%\etc\hadoop\mapred-site.xml.template
set OUTTEXTFILE=%HADOOP2_DIR%\etc\hadoop\mapred-site.xml
set SEARCHTEXT=__HS_IP__
set REPLACETEXT=%HS_IP%

if exist "%OUTTEXTFILE%" move /Y "%OUTTEXTFILE%" "%OUTTEXTFILE%-%date:~-4,4%-%date:~-7,2%-%date:~-10,2%-%time:~0,2%-%time:~3,2%-%time:~6,2%" > NUL
for /f "tokens=1,* delims=¶" %%A in ( '"type %INTEXTFILE%"') do (
        SET string=%%A
        setlocal EnableDelayedExpansion
        SET modified=!string:%SEARCHTEXT%=%REPLACETEXT%!

        >> %OUTTEXTFILE% echo(!modified!
            endlocal
            )

goto end




rem END Functions

:main
rem Set hadoop directory
set HADOOP2_DIR=%MY_MAPR_HOME%\hadoop\hadoop-%yarn_version%
rem Trim any whitespaces
for /l %%a in (1,1,100) do if "!HADOOP2_DIR:~-1!"==" " set HADOOP2_DIR=!HADOOP2_DIR:~0,-1!

goto configureHadoop

:DeQuote
for /f "delims=" %%A in ('echo %%%1%%') do set %1=%%~A
Goto :eof


:end

