@echo off
setlocal enabledelayedexpansion

echo Init...
terraform -chdir=infra\terraform init
if %ERRORLEVEL% neq 0 (
    echo ERROR: Terraform init failed
    exit /b 1
)

echo Validating...
terraform -chdir=infra\terraform validate
if %ERRORLEVEL% neq 0 (
    echo ERROR: Terraform validate failed
    exit /b 1
)

echo Planning...
terraform -chdir=infra\terraform plan
if %ERRORLEVEL% neq 0 (
    echo ERROR: Terraform plan failed
    exit /b 1
)

echo Provisioning Supabase (attempt 1/3)...
terraform -chdir=infra\terraform apply -auto-approve
if %ERRORLEVEL% equ 0 goto migrations

echo.
echo Supabase services still warming up, retrying in 10s (attempt 2/3)...
timeout /t 10 /nobreak >nul
terraform -chdir=infra\terraform apply -auto-approve
if %ERRORLEVEL% equ 0 goto migrations

echo.
echo Retrying in 10s (attempt 3/3)...
timeout /t 10 /nobreak >nul
terraform -chdir=infra\terraform apply -auto-approve
if %ERRORLEVEL% neq 0 (
    echo ERROR: Terraform apply failed after 3 attempts
    exit /b 1
)

:migrations
echo.
echo Running migrations...
uv run python infra\migrations\migrate.py
if %ERRORLEVEL% neq 0 (
    echo ERROR: Migrations failed
    exit /b 1
)

echo Done!
