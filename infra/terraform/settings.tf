resource "supabase_settings" "skypulse" {
  project_ref = supabase_project.skypulse.id   # toma el ref del proyecto recién creado

  api = jsonencode({
    db_schema            = "public,staging,intermediate,mart"
    db_extra_search_path = "public,extensions"
    max_rows             = 5000
  })
}
