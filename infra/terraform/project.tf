resource "supabase_project" "skypulse" {
  organization_id   = var.org_id
  name              = "SkyPulse-Streaming-Pipeline"
  database_password = var.db_password
  region            = var.supabase_region

  lifecycle {
    ignore_changes = [database_password]
  }
}

#terraform -chdir=infra/terraform init/validate/plan/apply
