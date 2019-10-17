cd C:\BDPL\scripts
git reset --hard
git pull

ICACLS "C:\BDPL\scripts\*" /q /c /t /reset

CD ..\scripts-test
git reset --hard
git pull

ICACLS "C:\BDPL\scripts-test\*" /q /c /t /reset

EXIT