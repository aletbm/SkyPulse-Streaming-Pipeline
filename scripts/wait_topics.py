import subprocess
import time

CONTAINER = "deploy-redpanda-1"
TOPICS = ["skypulse.seismic", "skypulse.flights", "skypulse.weather"]

print("Waiting for topics to be ready...")

while True:
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "rpk", "topic", "list"],
        capture_output=True,
        text=True,
    )
    if all(t in result.stdout for t in TOPICS):
        print("All topics ready!")
        break
    print("Not ready yet — retrying in 5s...")
    time.sleep(5)
