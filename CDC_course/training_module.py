import os
import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import pyspark
from time import time
from sklearn.cluster import KMeans
from pyspark.sql.functions import col, monotonically_increasing_id, rand
from pyspark.ml.feature import VectorAssembler, PCA
from pyspark.ml.stat import Correlation
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


def tune_hyperparameters(dataset_name, dataset, model_name, model, models_name_mapping, param_grids, file_name):
    results = read_results(file_name)
    if dataset_name not in results:
        results[dataset_name] = {}
    if model_name == 'mlp':
        train_data,_ = dataset
        features_number = len(train_data.columns) - 2
        param_grids['mlp'] = (ParamGridBuilder()
        .addGrid(model.layers, [[features_number, 50, 2], [features_number, 100, 2], [features_number, 25, 25, 2]])
        .addGrid(model.stepSize, [0.01, 0.1])
        .build())
    print(f'Training model {models_name_mapping[model_name]} on dataset {dataset_name}')
    if model_name not in results[dataset_name]:
        start = time()
        results[dataset_name][model_name] = fit_and_test_grid_search(model, *dataset, param_grids[model_name])
        results[dataset_name][model_name]['time'] = time()-start
        save_results(results, file_name)

    print(f'Accuracy:{results[dataset_name][model_name]["accuracy"]},\n\
        Precision:{results[dataset_name][model_name]["precision"]},\n\
        Recall:{results[dataset_name][model_name]["recall"]},\n\
        F1 Score:{results[dataset_name][model_name]["f1"]},\n\
        Time:{results[dataset_name][model_name]["time"]}seconds\n\
        Best Hyperparameters:{results[dataset_name][model_name]["best_params"]}\n\n')
    return results

def save_results(results, file_name):
    with open(file_name, 'w') as file:
        json.dump(results, file, indent=4)

def read_results(file_name):
    if os.path.exists(file_name):
        try:
            with open(file_name, 'r') as file:
                data = json.load(file)
                return data
        except json.JSONDecodeError:
            print(f"Error: {file_name} contains invalid JSON.")
            return {}
    else:
        return {}

def test_train_split(X_pyspark, y_pyspark):
    data_pyspark = X_pyspark.join(y_pyspark)
    train_data, test_data = data_pyspark.randomSplit([0.8, 0.2], seed=1234)
    assembler = VectorAssembler(inputCols=X_pyspark.columns, outputCol="features")
    train_data = assembler.transform(train_data)
    test_data = assembler.transform(test_data)
    return train_data, test_data

def fit_and_test_grid_search(model, train_data, test_data, param_grid):
    results = {}

    evaluator = BinaryClassificationEvaluator(labelCol="Diabetes_binary")
    crossval = CrossValidator(
        estimator=model,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3, parallelism=16
    )
    
    cv_model = crossval.fit(train_data) 

    best_params = {param.name: value for param, value in cv_model.bestModel.extractParamMap().items() if param in param_grid[0].keys()}

    results = test(cv_model, test_data)
    results['best_params'] = best_params
    return results

def fit_and_test(model, train_data, test_data):
    model = model.fit(train_data) 
    return test(model, test_data)

def fit_and_save_results(dataset_name, dataset, model_name, model, models_name_mapping, file_name):
    results = read_results(file_name)
    if dataset_name not in results:
        results[dataset_name] = {}
    print(f'Training model {models_name_mapping[model_name]} on dataset {dataset_name}')
    if model_name not in results[dataset_name]:
        start = time()
        results[dataset_name][model_name] = fit_and_test(model, *dataset)
        results[dataset_name][model_name]['time'] = time()-start
        save_results(results, file_name)

    print(f'Accuracy:{results[dataset_name][model_name]["accuracy"]},\n\
        Precision:{results[dataset_name][model_name]["precision"]},\n\
        Recall:{results[dataset_name][model_name]["recall"]},\n\
        F1 Score:{results[dataset_name][model_name]["f1"]},\n\
        Time:{results[dataset_name][model_name]["time"]}seconds\n\n')
    return results


def test(model,test_data):
    results = {}
    predictions = model.transform(test_data)

    accuracy_evaluator = MulticlassClassificationEvaluator(labelCol="Diabetes_binary", predictionCol="prediction", metricName="accuracy")
    results['accuracy'] = accuracy_evaluator.evaluate(predictions)

    f1_evaluator = MulticlassClassificationEvaluator(labelCol="Diabetes_binary", predictionCol="prediction", metricName="f1")
    results['f1'] = f1_evaluator.evaluate(predictions)

    recall_evaluator = MulticlassClassificationEvaluator(labelCol="Diabetes_binary", predictionCol="prediction", metricName="weightedRecall")
    results['recall'] = recall_evaluator.evaluate(predictions)

    precision_evaluator = MulticlassClassificationEvaluator(labelCol="Diabetes_binary", predictionCol="prediction", metricName="weightedPrecision")
    results['precision'] = precision_evaluator.evaluate(predictions)
    return results