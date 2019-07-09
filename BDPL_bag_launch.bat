@ECHO OFF

TYPE %3\scripts\bdpl.txt
ECHO.
ECHO.
ECHO.

if "%1."=="." GOTO No1
if "%2."=="." GOTO No1
if "%3."=="."  GOTO No2


setlocal EnableDelayedExpansion
IF NOT EXIST Y: (
  REM Get username
  SET /P _username="Enter your IU username: "

  REM Server passed in as CMD.EXE arg
  
  REM Connect to shared drive
  NET USE Y: %1 /user:ads\!_username! *
)

setlocal EnableDelayedExpansion
IF NOT EXIST Z: (
  REM Get username
  SET /P _username="Enter your IU username: "

  REM Server passed in as CMD.EXE arg
  
  REM Connect to shared drive
  NET USE Z: %2 /user:ads\!_username! *
)

CLS
python %3\scripts\bdpl_bag-prep.py
EXIT

:No1
ECHO Missing server address
ECHO.
PAUSE
EXIT

:No2
ECHO Missing local directory argument
ECHO.
PAUSE
EXIT