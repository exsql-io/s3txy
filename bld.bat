@echo off
set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
java -jar "%DIRNAME%/lib/bld/bld-wrapper.jar" "%0" --build io.exsql.s3xty.S3XtyBuild %*