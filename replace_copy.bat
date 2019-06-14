@ECHO OFF

REM Copy this file to %USERPROFILE%\AppData\Roaming\Microsoft\Windows\SendTo

SET _var=%~1

<NUL SET /P=%_var:\=/%| CLIP