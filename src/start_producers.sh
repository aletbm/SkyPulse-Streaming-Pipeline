#!/bin/bash

echo "🚀 Starting 3 producers..."

uv run python src/producers/flight_producer.py &
uv run python src/producers/seismic_producer.py &
uv run python src/producers/weather_producer.py &

echo "✅ All processes started"

wait
