#!/usr/bin/env bash
set -euo pipefail

echo "Init..."
terraform -chdir=infra/terraform init || { echo "ERROR: Terraform init failed"; exit 1; }

echo "Validating..."
terraform -chdir=infra/terraform validate || { echo "ERROR: Terraform validate failed"; exit 1; }

echo "Planning..."
terraform -chdir=infra/terraform plan || { echo "ERROR: Terraform plan failed"; exit 1; }

apply_terraform() {
    terraform -chdir=infra/terraform apply -auto-approve
}

echo "Provisioning Supabase (attempt 1/3)..."
if apply_terraform; then
    :
else
    echo ""
    echo "Supabase services still warming up, retrying in 30s (attempt 2/3)..."
    sleep 30
    if apply_terraform; then
        :
    else
        echo ""
        echo "Retrying in 30s (attempt 3/3)..."
        sleep 30
        if ! apply_terraform; then
            echo "ERROR: Terraform apply failed after 3 attempts"
            exit 1
        fi
    fi
fi

echo ""
echo "Running migrations..."
uv run python infra/migrations/migrate.py || { echo "ERROR: Migrations failed"; exit 1; }

echo "Done!"
