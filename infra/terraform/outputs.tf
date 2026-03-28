output "project_ref" {
  value       = supabase_project.skypulse.id
  description = "Usar como SUPABASE_PROJECT_REF en el resto del stack"
}

output "project_url" {
  value = "https://${supabase_project.skypulse.id}.supabase.co"
}

output "supabase_region" {
  value = var.supabase_region
  sensitive = true
}

output "db_password" {
  value     = var.db_password
  sensitive = true
}
