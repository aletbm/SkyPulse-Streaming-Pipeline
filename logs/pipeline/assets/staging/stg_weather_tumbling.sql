/* @bruin

name: staging.stg_weather_tumbling
type: pg.sql
connection: supabase

materialization:
  type: view

@bruin */

SELECT * FROM public.weather_tumbling;
