@ECHO OFF

TYPE C:\BDPL\scripts\bdpl.txt
ECHO.
ECHO.
ECHO.

if "%1." =="." GOTO No1

setlocal EnableDelayedExpansion
IF NOT EXIST Z: (
  ECHO Connecting to main BDPL workspace...
  ECHO.
  REM Get username
  SET /P _username="Enter your IU username: "

  REM Server passed in as CMD.EXE arg
  
  REM Connect to shared drive
  NET USE Z: %1 /user:ads\!_username! *
)

CLS
TYPE C:\BDPL\scripts\bdpl.txt
python C:\BDPL\scripts\bdpl_ripstation_ingest.py
EXIT

:No1
ECHO Missing server address
ECHO.
PAUSE
EXIT