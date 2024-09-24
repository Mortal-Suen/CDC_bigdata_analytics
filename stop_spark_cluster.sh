#!/bin/bash

# Define the container names
CONTAINERS=("spark-master" "spark-worker-1" "spark-worker-2")

# Stop the containers if they are running
for container in "${CONTAINERS[@]}"; do
  if [[ "$(docker ps --filter name=^/${container}$ --format="{{ .Names }}")" == "$container" ]]; then
    echo "Stopping container $container..."
    docker stop $container
  else
    echo "Container $container is not running."
  fi
done
