#!/bin/bash

# Define the container names
CONTAINERS=("spark-master" "spark-worker-1" "spark-worker-2")

# Remove the containers if they exist
for container in "${CONTAINERS[@]}"; do
  if [[ "$(docker ps -a --filter name=^/${container}$ --format="{{ .Names }}")" == "$container" ]]; then
    echo "Removing container $container..."
    docker rm -f $container
  else
    echo "Container $container does not exist."
  fi
done
