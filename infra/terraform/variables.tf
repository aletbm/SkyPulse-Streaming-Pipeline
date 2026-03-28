variable "supabase_access_token" {
  type      = string
  sensitive = true
}

variable "supabase_region" {
  type      = string
  sensitive = true
}

variable "org_id" {
  type        = string
  description = "Organization slug de Supabase"
}

variable "db_password" {
  type      = string
  sensitive = true
}
