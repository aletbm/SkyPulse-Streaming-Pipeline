@echo off
for /f "tokens=1,2 delims==" %%a in (.env) do set %%a=%%b

bruin run pipeline/pipeline.yml ^
  --end-date 2019-02-01 ^
  --full-refresh ^
  --workers 1 ^
  --environment default
