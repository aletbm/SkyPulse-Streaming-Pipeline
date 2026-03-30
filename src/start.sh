#!/bin/bash

echo "🚀 Starting 3 producers..."

uv run python src/producers/flight_producer.py &
uv run python src/producers/seismic_producer.py &
uv run python src/producers/weather_producer.py &

echo "📥 Starting 3 consumers..."

uv run python src/consumers/flight_consumer.py &
uv run python src/consumers/seismic_consumer.py &
uv run python src/consumers/weather_consumer.py &

echo "✅ All processes started"

wait
