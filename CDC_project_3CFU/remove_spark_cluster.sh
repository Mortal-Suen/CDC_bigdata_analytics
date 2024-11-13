#!/bin/bash

# Set the image and network names
IMAGE="bitnami/spark:latest"
NETWORK="spark-net"

# Check if the number of workers is provided as an argument, default to 4
NUM_WORKERS=${1:-4}

# Define the container names
CONTAINERS=("spark-master")
for i in $(seq 1 $NUM_WORKERS); do
  CONTAINERS+=("spark-worker-$i")
done

# Remove the containers if they exist
for container in "${CONTAINERS[@]}"; do
  if [[ "$(docker ps -a --filter name=^/${container}$ --format="{{ .Names }}")" == "$container" ]]; then
    echo "Removing container $container..."
    docker rm -f $container
  else
    echo "Container $container does not exist."
  fi
done
