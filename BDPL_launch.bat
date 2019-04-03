@ECHO OFF

if "%1." =="." GOTO No1
if "%2." =="."  GOTO No2

TYPE %2\scripts\bdpl.txt
ECHO.
ECHO.
ECHO.

setlocal EnableDelayedExpansion
IF NOT EXIST Z: (
  REM Get username
  SET /P _username="Enter your IU username: "

  REM Server passed in as CMD.EXE arg
  
  REM Connect to shared drive
  NET USE Z: %1 /user:ads\!_username! *
)

CLS
TYPE %2\scripts\bdpl.txt
python %2\scripts\bdpl_ingest.py
EXIT

:No1
ECHO Missing local directory argument
ECHO.
PAUSE
EXIT

:No2
ECHO Missing server address
ECHO.
PAUSE
EXIT
