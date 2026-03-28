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

echo Provisioning Supabase...
terraform -chdir=infra\terraform apply -auto-approve
if %ERRORLEVEL% neq 0 (
    echo ERROR: Terraform apply failed
    exit /b 1
)

echo Running migrations...
uv run python infra\migrations\migrate.py
if %ERRORLEVEL% neq 0 (
    echo ERROR: Migrations failed
    exit /b 1
)

echo Done!
