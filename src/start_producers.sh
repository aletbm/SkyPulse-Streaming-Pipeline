#!/bin/bash

echo "🚀 Starting 3 producers..."

source .venv/bin/activate

python src/producers/flight_producer.py &
sleep 1
python src/producers/seismic_producer.py &
sleep 1
python src/producers/weather_producer.py &

echo "✅ All processes started"

wait
