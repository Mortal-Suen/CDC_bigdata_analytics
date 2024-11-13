#!/bin/bash

# Set the image and network names
IMAGE="bitnami/spark:latest"
NETWORK="spark-net"

# Check if a number of workers is passed as an argument; default to 4 if not
NUM_WORKERS=${1:-4}  # Use the first argument or default to 4
CORES_PER_WORKER=2

if ! [[ "$NUM_WORKERS" =~ ^[0-9]+$ ]]; then
  echo "Invalid number of workers. Using default value of 4."
  NUM_WORKERS=4
fi

# Create an array of container names based on the number of workers
CONTAINERS=("spark-master")
for i in $(seq 1 $NUM_WORKERS); do
  CONTAINERS+=("spark-worker-$i")
done

# Check if the image is already pulled
if [[ "$(docker images -q $IMAGE 2> /dev/null)" == "" ]]; then
  echo "Image $IMAGE not found. Pulling the image..."
  docker pull $IMAGE
else
  echo "Image $IMAGE already exists. Skipping pull..."
fi

# Check if the network exists, and create it if it doesn't
if [[ "$(docker network ls --filter name=^${NETWORK}$ --format="{{ .Name }}")" == "$NETWORK" ]]; then
  echo "Network $NETWORK already exists. Skipping network creation..."
else
  echo "Network $NETWORK not found. Creating the network..."
  docker network create $NETWORK
fi

# Remove existing containers with the same names
for container in "${CONTAINERS[@]}"; do
  if [[ "$(docker ps -a --filter name=^/${container}$ --format="{{ .Names }}")" == "$container" ]]; then
    echo "Container $container already exists. Removing the container..."
    docker rm -f $container
  fi
done

# Run the Spark master container
docker run -d --name spark-master --network $NETWORK \
  -e SPARK_MODE=master -p 8080:8080 -p 7077:7077 $IMAGE

# Run the Spark worker containers
for i in $(seq 1 $NUM_WORKERS); do
  PORT=$((8080 + i))
  docker run -d --name spark-worker-$i --network $NETWORK \
    -e SPARK_MODE=worker -e SPARK_MASTER_URL=spark://spark-master:7077 \
    -e SPARK_WORKER_CORES=$CORES_PER_WORKER \
    -p $PORT:8081 $IMAGE
done

echo "$NUM_WORKERS Spark worker(s) started."
