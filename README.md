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

## Run jupyter notebook with the Spark Cluster

1. Run this command in order to get the ip adress of the spark master
```sh
sudo docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark-master
```

2. In the notebook, where the spark app is started, change 
```python
spark = SparkSession.builder \
    .appName("CDC Diabetes Health Indicators") \
    .master('local[16]') \
    .config("spark.driver.memory", "16g")\
    .getOrCreate()
```
to
```python
spark = SparkSession.builder \
    .appName("CDC Diabetes Health Indicators") \
    .master('spark://master_ip:7077') \
    .config("spark.driver.memory", "16g")\
    .getOrCreate()
```
where master_ip is the ip adress you got from the command above