@ECHO OFF

if "%2" == "" (
    ECHO Add local address of files
)   else (
    ECHO %1
)

TYPE %2\scripts\bdpl.txt
ECHO.
ECHO.
ECHO.

if "%1" == "" (
    ECHO Add server address as argument
)   else (
    ECHO %1
)

IF NOT EXIST Z: (
  REM Get username
  SET /P _username="Enter your IU username: "

  REM Server passed in as CMD.EXE arg
  
  REM Connect to shared drive
  NET USE Z: %1 /user:ads\%_username% *
)

CLS
TYPE %2\scripts\bdpl.txt
python %2\scripts\bdpl_ingest.py




