#!/bin/bash

echo "📥 Starting 4 jobs..."

source .venv/bin/activate

python src/jobs/jobs_quix/flight_context_tumbling.py &
sleep 1
python src/jobs/jobs_quix/seismic_tumbling.py &
sleep 1
python src/jobs/jobs_quix/weather_tumbling.py &
sleep 1
python src/jobs/jobs_quix/flight_tumbling.py &

echo "✅ All processes started"

wait
