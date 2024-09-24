#!/bin/bash

# Set the image and network names
IMAGE="bitnami/spark:latest"
NETWORK="spark-net"
CONTAINERS=("spark-master" "spark-worker-1" "spark-worker-2")

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
docker run -d --name spark-worker-1 --network $NETWORK \
  -e SPARK_MODE=worker -e SPARK_MASTER_URL=spark://spark-master:7077 -p 8081:8081 $IMAGE

docker run -d --name spark-worker-2 --network $NETWORK \
  -e SPARK_MODE=worker -e SPARK_MASTER_URL=spark://spark-master:7077 -p 8082:8082 $IMAGE
