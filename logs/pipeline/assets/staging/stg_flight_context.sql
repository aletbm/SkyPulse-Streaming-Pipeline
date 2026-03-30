/* @bruin

name: staging.stg_flight_context
type: pg.sql
connection: supabase

materialization:
  type: view

@bruin */

SELECT * FROM public.flight_context;
