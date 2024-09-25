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

## Run jupyter notebook

1. You need to run a docker container for the notebook.
```sh
docker run --network spark-net -d -p 8888:8888 \
-e JUPYTER_ENABLE_LAB=yes \
-e SPARK_MASTER_URL=spark://spark-master:7077 \
--name jupyter-notebook \
jupyter/pyspark-notebook

```
2. Run the following command.
```sh
docker logs jupyter-notebook
```
3. Search for line:
http://127.0.0.1:8888/lab?token=<token>
4. Click on it in order to go to the jupyter notebook page.
5. Upload your notebook and run it.
6. When you are done, download your new version of the notebook, stop and remove the container.
```sh
docker stop jupyter-notebook
docker rm jupyter-notebook
```