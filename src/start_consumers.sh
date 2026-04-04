#!/bin/bash

echo "📥 Starting 3 consumers..."

source .venv/bin/activate

python src/consumers/flight_consumer.py &
sleep 1
python src/consumers/seismic_consumer.py &
sleep 1
python src/consumers/weather_consumer.py &

echo "✅ All processes started"

wait
