@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.


@rem This script runs the hadoop core commands. 

@rem Environment Variables
@rem
@rem   JAVA_HOME        The java implementation to use.  Overrides JAVA_HOME.
@rem
@rem   HADOOP_CLASSPATH Extra Java CLASSPATH entries.
@rem
@rem   HADOOP_USER_CLASSPATH_FIRST      When defined, the HADOOP_CLASSPATH is
@rem                                    added in the beginning of the global
@rem                                    classpath. Can be defined, for example,
@rem                                    by doing
@rem                                    export HADOOP_USER_CLASSPATH_FIRST=true
@rem
@rem   HADOOP_HEAPSIZE  The maximum amount of heap to use, in MB.
@rem                    Default is 1000.
@rem
@rem   HADOOP_OPTS      Extra Java runtime options.
@rem
@rem   HADOOP_CLIENT_OPTS         when the respective command is run.
@rem   HADOOP_{COMMAND}_OPTS etc  HADOOP_JT_OPTS applies to JobTracker
@rem                              for e.g.  HADOOP_CLIENT_OPTS applies to
@rem                              more than one command (fs, dfs, fsck,
@rem                              dfsadmin etc)
@rem
@rem   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
@rem
@rem   HADOOP_ROOT_LOGGER The root appender. Default is INFO,console
@rem

@rem Begin: MapR specific section
if not defined MAPR_HOME (
  echo MAPR_HOME is not set. Set MAPR_HOME to the installation folder and run this command.
  goto end
)

@rem bug 6908 : Source env variables
set MY_MAPR_HOME=%MAPR_HOME%
set FC=%MY_MAPR_HOME:~0,1%

if [!FC!]==[^"] (
  set MY_MAPR_HOME=%MAPR_HOME:~1,-1%
)

if exist "%MY_MAPR_HOME%\conf\env.bat" (
  call "%MY_MAPR_HOME%\conf\env.bat"
)
@rem End: MapR specific section

if not defined HADOOP_BIN_PATH ( 
  set HADOOP_BIN_PATH=%~dp0
)

if "%HADOOP_BIN_PATH:~-1%" == "\" (
  set HADOOP_BIN_PATH=%HADOOP_BIN_PATH:~0,-1%
)

call :updatepath %HADOOP_BIN_PATH%

@rem Begin: MapR specific section
if exist %MAPR_HOME%\lib (
  set CLASSPATH=!CLASSPATH!;%MAPR_HOME%\lib\kvstore-0.1.jar
  set CLASSPATH=!CLASSPATH!;%MAPR_HOME%\lib\libprotodefs.jar
  set CLASSPATH=!CLASSPATH!;%MAPR_HOME%\lib\baseutils-0.1.jar
  set CLASSPATH=!CLASSPATH!;%MAPR_HOME%\lib\maprsecurity-0.1.jar
  set CLASSPATH=!CLASSPATH!;%MAPR_HOME%\lib\json-20080701.jar
  set CLASSPATH=!CLASSPATH!;%MAPR_HOME%\lib\json-1.8.jar
  set CLASSPATH=!CLASSPATH!;%MAPR_HOME%\lib\flexjson-2.1.jar
)

@rem Bug 10369 - Fix to run HBase shell, etc in windows
if not "%HADOOP_CLASSPATH%" == "" (
  set CLASSPATH=%CLASSPATH%;%HADOOP_CLASSPATH%
)
@rem End: MapR specific section

:main
  setlocal enabledelayedexpansion

  set DEFAULT_LIBEXEC_DIR=%HADOOP_BIN_PATH%\..\libexec
  if not defined HADOOP_LIBEXEC_DIR (
    set HADOOP_LIBEXEC_DIR=%DEFAULT_LIBEXEC_DIR%
  )

  call %HADOOP_LIBEXEC_DIR%\hadoop-config.cmd %*
  if "%1" == "--config" (
    shift
    shift
  )
  if "%1" == "--loglevel" (
    shift
    shift
  )

  set hadoop-command=%1
  if not defined hadoop-command (
      goto print_usage
  )

  call :make_command_arguments %*

  set hdfscommands=namenode secondarynamenode datanode dfs dfsadmin fsck balancer fetchdt oiv dfsgroups
  for %%i in ( %hdfscommands% ) do (
    if %hadoop-command% == %%i set hdfscommand=true
  )
  if defined hdfscommand (
    @echo DEPRECATED: Use of this script to execute hdfs command is deprecated. 1>&2
    @echo Instead use the hdfs command for it. 1>&2
    if exist %HADOOP_HDFS_HOME%\bin\hdfs.cmd (
      call %HADOOP_HDFS_HOME%\bin\hdfs.cmd %*
      goto :eof
    ) else if exist %HADOOP_HOME%\bin\hdfs.cmd (
      call %HADOOP_HOME%\bin\hdfs.cmd %*
      goto :eof
    ) else (
      echo HADOOP_HDFS_HOME not found!
      goto :eof
    )
  )

  set mapredcommands=pipes job queue mrgroups mradmin jobtracker tasktracker
  for %%i in ( %mapredcommands% ) do (
    if %hadoop-command% == %%i set mapredcommand=true  
  )
  if defined mapredcommand (
    @echo DEPRECATED: Use of this script to execute mapred command is deprecated. 1>&2
    @echo Instead use the mapred command for it. 1>&2
    if exist %HADOOP_MAPRED_HOME%\bin\mapred.cmd (
      call %HADOOP_MAPRED_HOME%\bin\mapred.cmd %*
      goto :eof
    ) else if exist %HADOOP_HOME%\bin\mapred.cmd (
      call %HADOOP_HOME%\bin\mapred.cmd %*
      goto :eof
    ) else (
      echo HADOOP_MAPRED_HOME not found!
      goto :eof
    )
  )

  if %hadoop-command% == classpath (
    if not defined hadoop-command-arguments (
      @rem No need to bother starting up a JVM for this simple case.
      @echo %CLASSPATH%
      exit /b
    )
  )

  @rem Begin: MapR specific section
  if %hadoop-command% == jnipath (
    @echo %JAVA_LIBRARY_PATH%
    goto :eof
  )
  @rem End: MapR specific section
  
  set corecommands=fs version jar checknative distcp daemonlog archive classpath credential key
  for %%i in ( %corecommands% ) do (
    if %hadoop-command% == %%i set corecommand=true  
  )
  if defined corecommand (
    call :%hadoop-command%
  ) else (
    set CLASSPATH=%CLASSPATH%;%CD%
    set CLASS=%hadoop-command%
  )

  set path=%PATH%;%HADOOP_BIN_PATH%

  @rem Always respect HADOOP_OPTS and HADOOP_CLIENT_OPTS
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%

  @rem make sure security appender is turned off
  if not defined HADOOP_SECURITY_LOGGER (
    set HADOOP_SECURITY_LOGGER=INFO,NullAppender
  )
  set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.security.logger=%HADOOP_SECURITY_LOGGER%

  call %JAVA% %JAVA_HEAP_MAX% %HADOOP_OPTS% -classpath %CLASSPATH% %CLASS% %hadoop-command-arguments%

  exit /b %ERRORLEVEL%

:fs 
  set CLASS=org.apache.hadoop.fs.FsShell
  goto :eof

:version 
  set CLASS=org.apache.hadoop.util.VersionInfo
  goto :eof

:jar
  if defined YARN_OPTS (
    @echo WARNING: Use "yarn jar" to launch YARN applications. 1>&2
  ) else if defined YARN_CLIENT_OPTS (
    @echo WARNING: Use "yarn jar" to launch YARN applications. 1>&2
  )
  set CLASS=org.apache.hadoop.util.RunJar
  goto :eof

:checknative
  set CLASS=org.apache.hadoop.util.NativeLibraryChecker
  goto :eof

:distcp
  set CLASS=org.apache.hadoop.tools.DistCp
  set CLASSPATH=%CLASSPATH%;%TOOL_PATH%
  goto :eof

:daemonlog
  set CLASS=org.apache.hadoop.log.LogLevel
  goto :eof

:archive
  set CLASS=org.apache.hadoop.tools.HadoopArchives
  set CLASSPATH=%CLASSPATH%;%TOOL_PATH%
  goto :eof

:classpath
  set CLASS=org.apache.hadoop.util.Classpath
  goto :eof

:credential
  set CLASS=org.apache.hadoop.security.alias.CredentialShell
  goto :eof

:key
  set CLASS=org.apache.hadoop.crypto.key.KeyShell
  goto :eof

@rem Begin: MapR specific section

:mfs
  set CLASS=com.mapr.fs.clicommands.MapRCliCommands
  goto :eof

:conf
  set CLASS=org.apache.hadoop.mapreduce.util.ConfigUtil
  goto :eof

@rem End: MapR specific section

:updatepath
  set path_to_add=%*
  set current_path_comparable=%path%
  set current_path_comparable=%current_path_comparable: =_%
  set current_path_comparable=%current_path_comparable:(=_%
  set current_path_comparable=%current_path_comparable:)=_%
  set path_to_add_comparable=%path_to_add%
  set path_to_add_comparable=%path_to_add_comparable: =_%
  set path_to_add_comparable=%path_to_add_comparable:(=_%
  set path_to_add_comparable=%path_to_add_comparable:)=_%

  for %%i in ( %current_path_comparable% ) do (
    if /i "%%i" == "%path_to_add_comparable%" (
      set path_to_add_exist=true
    )
  )
  set system_path_comparable=
  set path_to_add_comparable=
  if not defined path_to_add_exist path=%path_to_add%;%path%
  set path_to_add=
  goto :eof

@rem This changes %1, %2 etc. Hence those cannot be used after calling this.
:make_command_arguments
  if "%1" == "--config" (
    shift
    shift
  )
  if "%1" == "--loglevel" (
    shift
    shift
  )
  if [%2] == [] goto :eof
  shift
  set _arguments=
  :MakeCmdArgsLoop 
  if [%1]==[] goto :EndLoop 

  if not defined _arguments (
    set _arguments=%1
  ) else (
    set _arguments=!_arguments! %1
  )
  shift
  goto :MakeCmdArgsLoop 
  :EndLoop 
  set hadoop-command-arguments=%_arguments%
  goto :eof

:print_usage
  @echo Usage: hadoop [--config confdir] [--loglevel loglevel] COMMAND
  @echo where COMMAND is one of:
  @echo   fs                   run a generic filesystem user client
  @echo   mfs                  run MapR-FS commands
  @echo   version              print the version
  @echo   jar ^<jar^>            run a jar file
  @echo                        note: please use "yarn jar" to launch
  @echo                              YARN applications, not this command.
  @echo   checknative [-a^|-h]  check native hadoop and compression libraries availability
  @echo   distcp ^<srcurl^> ^<desturl^> copy file or directories recursively
  @echo   archive -archiveName NAME -p ^<parent path^> ^<src^>* ^<dest^> create a hadoop archive
  @echo   classpath            prints the class path needed to get the
  @echo                        Hadoop jar and the required libraries
  @echo   credential           interact with credential providers
  @echo   key                  manage keys via the KeyProvider
  @echo   jnipath              prints the path to the native libraries
  @echo   conf                 print Hadoop configuration keys
  @echo   daemonlog            get/set the log level for each daemon
  @echo  or
  @echo   CLASSNAME            run the class named CLASSNAME
  @echo.
  @echo Most commands print help when invoked w/o parameters.

endlocal
