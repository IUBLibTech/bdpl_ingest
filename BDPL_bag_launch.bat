@ECHO OFF

TYPE C:\BDPL\scripts\bdpl.txt
ECHO.
ECHO.
ECHO.

if "%1."=="." GOTO No1
if "%2."=="." GOTO No1


setlocal EnableDelayedExpansion
IF NOT EXIST Y: (
  ECHO Connecting to Archiver Spool location...
  ECHO.
  REM Get username
  SET /P _username="Enter your IU username: "

  REM Server passed in as CMD.EXE arg
  
  REM Connect to shared drive
  NET USE Y: %1 /user:ads\!_username! *
)

setlocal EnableDelayedExpansion
IF NOT EXIST Z: (
  ECHO.
  ECHO.
  ECHO Connecting to main BDPL workspace...
  ECHO.
  REM Get username
  SET /P _username="Enter your IU username: "

  REM Server passed in as CMD.EXE arg
  
  REM Connect to shared drive
  NET USE Z: %2 /user:ads\!_username! *
)

CLS
python C:\BDPL\scripts\bdpl_bag-prep.py
ECHO.
ECHO.
ECHO.
PAUSE
EXIT /B

:No1
ECHO Missing server address
ECHO.
PAUSE
EXIT