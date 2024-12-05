# CDC_bigdata_analytics

## Building and Running Spark Cluster
We created a Spark cluster with a Master node and two Worker nodes using Docker containers. You can run it using the following command.
```sh
sudo ./run_spark_cluster.sh
```
or
```sh
sudo ./run_spark_cluster.sh <number_of_workers>
```
The default numbe rof workers is 4.
You can validate that the cluster is running by visiting the web UI of the Spark Master on [localhost:8080](http://localhost:8080/). 

You can stop the cluster using this command.
```sh
sudo ./stop_spark_cluster.sh
```
or
```sh
sudo ./stop_spark_cluster.sh <number_of_workers>
```
You can remove the Docker containers using this command.
```sh
sudo ./remove_spark_cluster.sh
```
or
```sh
sudo ./remove_spark_cluster.sh <number_of_workers>
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

## Domain Knowledge Resources

In the domain of diabetes research, certain health indicators, lifestyle factors, and demographic attributes are well-established as having a strong correlation with diabetes risk. Here's an overview of domain knowledge on the most critical metrics, supported by relevant findings from scientific literature.

### Key Metrics Known to Influence Diabetes

1. **Body Mass Index (BMI)**
   - **Importance**: BMI is one of the strongest predictors of type 2 diabetes risk. Higher BMI values are associated with increased insulin resistance and the development of diabetes.
   - **Support**:  
     - Hironobu Sanada, Hirohide Yokokawa, Minoru Yoneda, Junichi Yatabe, Midori Sasaki Yatabe, Scott M. Williams, Robin A. Felder, and Pedro A. Jose. "High Body Mass Index is an Important Risk Factor for the Development of Type 2 Diabetes." *Internal Medicine*, vol. 51, no. 14, 2012, pp. 1821–1826. doi: [10.2169/internalmedicine.51.7410](https://doi.org/10.2169/internalmedicine.51.7410)
   - **Action**: Treat BMI as a key feature in your model and consider assigning higher weight in a composite risk score.

2. **High Blood Pressure (HighBP)**
   - **Importance**: High blood pressure is closely linked to the metabolic syndrome, a cluster of conditions that includes diabetes. Many patients with diabetes also have hypertension.
   - **Support**:  
     - Sowers JR, Epstein M, Frohlich ED. "Diabetes, Hypertension, and Cardiovascular Disease: An Update." *Hypertension*, vol. 37, no. 4, 2001, pp. 1053–1059. doi: [10.1161/01.hyp.37.4.1053](https://doi.org/10.1161/01.hyp.37.4.1053)
   - **Action**: Include HighBP as a primary risk indicator, possibly combining it with other cardiovascular-related variables like cholesterol and heart disease.

3. **High Cholesterol (HighChol)**
   - **Importance**: High levels of LDL cholesterol and low levels of HDL cholesterol are risk factors for both cardiovascular disease and diabetes.
   - **Support**:  
     - Mohamed E, Mohamed M, Rashid FA. "Dyslipidaemic Pattern of Patients with Type 2 Diabetes Mellitus." *Malaysian Journal of Medical Sciences*, vol. 11, no. 1, 2004, pp. 44–51. PMID: [22977359](https://pubmed.ncbi.nlm.nih.gov/22977359)
   - **Action**: Consider merging HighChol with HighBP and other cardiovascular factors to create a composite cardiovascular risk score.

4. **Physical Activity (PhysActivity)**
   - **Importance**: Regular physical activity has been shown to reduce insulin resistance and lower the risk of developing diabetes.
   - **Support**:  
     - Helmrich SP, Ragland DR, Leung RW, and Paffenbarger RS Jr. "Physical Activity and Reduced Occurrence of Non-Insulin-Dependent Diabetes Mellitus." *New England Journal of Medicine*, vol. 325, no. 3, 1991, pp. 147–152. doi: [10.1056/NEJM199107183250302](https://doi.org/10.1056/NEJM199107183250302)
   - **Action**: Physical activity should be an essential variable in predictive models. You can combine it with BMI and dietary indicators for a holistic health/lifestyle index.

5. **Diet (Fruits and Veggies)**
   - **Importance**: A diet rich in fruits and vegetables is associated with a lower risk of diabetes due to their role in improving insulin sensitivity and overall health.
   - **Support**:  
     - Halvorsen RE, Elvestad M, Molin M, and Aune D. "Fruit and Vegetable Consumption and the Risk of Type 2 Diabetes: A Systematic Review and Dose-Response Meta-Analysis of Prospective Studies." *BMJ Nutrition, Prevention & Health*, vol. 4, no. 2, 2021, pp. 519–531. doi: [10.1136/bmjnph-2020-000218](https://doi.org/10.1136/bmjnph-2020-000218)
   - **Action**: Merging Fruits and Veggies into a dietary score can be useful, as diet plays a critical role in diabetes prevention and management.

6. **Smoking and Alcohol Consumption (Smoker, HvyAlcoholConsump)**
   - **Importance**: Both smoking and heavy alcohol consumption are associated with increased risk of type 2 diabetes. Smoking increases insulin resistance, while excessive alcohol can impair glucose metabolism.
   - **Support**:  
     - Willi C, Bodenmann P, Ghali WA, Faris PD, and Cornuz J. "Active Smoking and the Risk of Type 2 Diabetes: A Systematic Review and Meta-Analysis." *JAMA*, vol. 298, no. 22, 2007, pp. 2654–2664. doi: [10.1001/jama.298.22.2654](https://doi.org/10.1001/jama.298.22.2654)  
     - Crandall JP, Polsky S, Howard AA, et al. "Alcohol Consumption and Diabetes Risk in the Diabetes Prevention Program." *American Journal of Clinical Nutrition*, vol. 90, no. 3, 2009, pp. 595–601. doi: [10.3945/ajcn.2008.27382](https://doi.org/10.3945/ajcn.2008.27382)
   - **Action**: Combining Smoker and HvyAlcoholConsump into an unhealthy behavior index can highlight risk factors for diabetes.

7. **Age and Gender**
   - **Importance**: Age is a major risk factor, with older populations showing a significantly higher prevalence of diabetes. Gender also plays a role, as men and women may exhibit different risk profiles.
   - **Support**:  
     - Huebschmann AG, Huxley RR, Kohrt WM, Zeitler P, Regensteiner JG, and Reusch JEB. "Sex Differences in the Burden of Type 2 Diabetes and Cardiovascular Risk across the Life Course." *Diabetologia*, vol. 62, no. 10, 2019, pp. 1761–1772. doi: [10.1007/s00125-019-4939-5](https://doi.org/10.1007/s00125-019-4939-5)
   - **Action**: Age should be included as a categorical or continuous feature, while gender could be included as part of interaction terms to explore potential differences.

8. **Mental and Physical Health (MentHlth, PhysHlth)**
   - **Importance**: Mental and physical health are increasingly recognized as interrelated risk factors. Depression and poor physical health can exacerbate diabetes risk and complications.
   - **Support**:  
     - Alzoubi A, Abunaser R, Khassawneh A, et al. "The Bidirectional Relationship between Diabetes and Depression: A Literature Review." *Korean Journal of Family Medicine*, vol. 39, no. 3, 2018, pp. 137–146. doi: [10.4082/kjfm.2018.39.3.137](https://doi.org/10.4082/kjfm.2018.39.3.137)
   - **Action**: These features can be combined into a general health index or used to explain variance in diabetes outcomes.

9. **Healthcare Access (AnyHealthcare, NoDocbcCost)**
   - **Importance**: Access to healthcare influences the diagnosis and management of diabetes, as well as access to preventive care.
   - **Support**:  
     - Zhang X, Bullard KM, Gregg EW, et al. "Access to Health Care and Control of ABCs of Diabetes." *Diabetes Care*, vol. 35, no. 7, 2012, pp. 1566–1571. doi: [10.2337/dc12-0081](https://doi.org/10.2337/dc12-0081)
   - **Action**: Merging these indicators into a healthcare access score may highlight socioeconomic barriers that affect diabetes outcomes.
