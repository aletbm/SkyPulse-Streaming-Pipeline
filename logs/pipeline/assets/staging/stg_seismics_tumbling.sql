/* @bruin

name: staging.stg_seismics_tumbling
type: pg.sql
connection: supabase

materialization:
  type: view

@bruin */

SELECT * FROM public.seismics_tumbling;
