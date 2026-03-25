/* @bruin

name: staging.stg_flights_tumbling
type: pg.sql
connection: supabase

materialization:
  type: view

@bruin */

SELECT * FROM public.flights_tumbling;
