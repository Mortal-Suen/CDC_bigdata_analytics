# CDC_bigdata_analytics

## Building and Running Spark Cluster
We created a Spark cluster with a Master node and two Worker nodes using Docker containers. You can run it using the following command.
```sh
sudo ./run_spark_cluster.sh
```
You can validate that the cluster is running by visiting the web UI of the Spark Master on [localhost:8080](http://localhost:8080/). 

You can stop the cluster using this command.
```sh
sudo ./stop_spark_cluster.sh
```
You can remove the Docker containers using this command.
```sh
sudo ./remove_spark_cluster.sh
```
