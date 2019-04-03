@ECHO OFF
TYPE C:\BDPL\scripts\bdpl.txt
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
TYPE C:\BDPL\scripts\bdpl.txt
python C:\BDPL\scripts\bdpl_ingest.py




