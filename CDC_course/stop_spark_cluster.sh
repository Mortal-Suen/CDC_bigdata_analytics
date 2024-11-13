#!/bin/bash

# Check if the number of workers is provided as an argument, default to 4
NUM_WORKERS=${1:-4}

# Define the container names
CONTAINERS=("spark-master")
for i in $(seq 1 $NUM_WORKERS); do
  CONTAINERS+=("spark-worker-$i")
done

# Stop the containers if they are running
for container in "${CONTAINERS[@]}"; do
  if [[ "$(docker ps --filter name=^/${container}$ --format="{{ .Names }}")" == "$container" ]]; then
    echo "Stopping container $container..."
    docker stop $container
  else
    echo "Container $container is not running."
  fi
done
