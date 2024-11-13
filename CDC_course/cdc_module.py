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


def clean_data(X, y):
    # Remove duplicate rows
    combined = pd.concat([X, y], axis=1).drop_duplicates()

    # Check for identical X with different y and remove them
    inconsistent_indices = combined[combined.duplicated(subset=combined.columns[:-1], keep=False) & combined.duplicated(subset=[combined.columns[-1]], keep=False)].index
    if not inconsistent_indices.empty:
        combined = combined.drop(inconsistent_indices)

    # Separate features and target after cleaning
    X_cleaned = combined.iloc[:, :-1]
    y_cleaned = pd.DataFrame(combined.iloc[:, -1], columns=['Diabetes_binary'])
    
    return X_cleaned, y_cleaned

def spark_histograms(X_pyspark):
    # Get the number of columns
    num_columns = len(X_pyspark.columns)
    num_rows = (num_columns // 5) + (1 if num_columns % 5 != 0 else 0)

    # Create a 5x5 subgraph layout
    fig, axes = plt.subplots(num_rows, 5, figsize=(25, 5 * num_rows))
    axes = axes.flatten()

    # Iterate through each column and compute the distribution data, which is then plotted in the subplot
    for i, column in enumerate(X_pyspark.columns):
        # Use Spark to select the column and collect the data
        column_data = X_pyspark.select(column).rdd.flatMap(lambda x: x).collect()
        
        # Plotting histograms in the corresponding subplots
        sns.histplot(column_data, kde=True, bins=30, ax=axes[i])
        axes[i].set_title(f'Distribution of {column}')
        axes[i].set_xlabel(column)
        axes[i].set_ylabel('Frequency')

    # Remove redundant subgraphs (if there are blank subgraphs)
    for i in range(num_columns, len(axes)):
        fig.delaxes(axes[i])

    # Adjust the layout to avoid overlapping subgraphs
    plt.tight_layout()
    plt.show()

def validity_check(X_pyspark):
    # Define validity checks using a list of conditions
    validity_checks = [
        ("HighBP", lambda c: (c == 0) | (c == 1)),
        ("HighChol", lambda c: (c == 0) | (c == 1)),
        ("CholCheck", lambda c: (c == 0) | (c == 1)),
        # ("BMI", lambda c: (c >= 10) & (c <= 80)),  # Example reasonable range
        ("Smoker", lambda c: (c == 0) | (c == 1)),
        ("Stroke", lambda c: (c == 0) | (c == 1)),
        ("HeartDiseaseorAttack", lambda c: (c == 0) | (c == 1)),
        ("PhysActivity", lambda c: (c == 0) | (c == 1)),
        ("Fruits", lambda c: (c == 0) | (c == 1)),
        ("Veggies", lambda c: (c == 0) | (c == 1)),
        ("HvyAlcoholConsump", lambda c: (c == 0) | (c == 1)),
        ("AnyHealthcare", lambda c: (c == 0) | (c == 1)),
        ("NoDocbcCost", lambda c: (c == 0) | (c == 1)),
        ("GenHlth", lambda c: (c >= 1) & (c <= 5)),
        ("MentHlth", lambda c: (c >= 0) & (c <= 30)),
        ("PhysHlth", lambda c: (c >= 0) & (c <= 30)),
        ("DiffWalk", lambda c: (c == 0) | (c == 1)),
        ("Sex", lambda c: (c == 0) | (c == 1)),
        ("Age", lambda c: (c >= 1) & (c <= 13)),
        ("Education", lambda c: (c >= 1) & (c <= 6)),
        ("Income", lambda c: (c >= 1) & (c <= 8))
    ]

    # Iterate over each validity check
    for column_name, validity_check in validity_checks:
        # Filter rows that do not meet the validity check and count them
        invalid_count = X_pyspark.filter(~validity_check(col(column_name))).count()
        print(f"Feature '{column_name}' has {invalid_count} invalid entries.")

def target_histograms(y_pyspark):
    # Iterate through each column and compute the histogram data
    for column in y_pyspark.columns:
        # Use the select and rdd methods of the Spark DataFrame to get the data for this column
        column_data = y_pyspark.select(column).rdd.flatMap(lambda x: x).collect()
        
        # Plot the histogram of the column
        plt.figure(figsize=(10, 5))
        sns.histplot(column_data, kde=True, bins=30)
        plt.title(f'Distribution of {column}')
        plt.xlabel(column)
        plt.ylabel('Frequency')
        plt.grid(True)
        plt.tight_layout()
        plt.show()

def calculate_correlation(X_pyspark, y_pyspark, corr_mod='spearman'):
    # Add a unique index column to both DataFrames to ensure row-wise splicing
    X_with_id = X_pyspark.withColumn("id", monotonically_increasing_id())
    y_with_id = y_pyspark.withColumn("id", monotonically_increasing_id())

    # Splice features and targets together, matching by 'id' columns
    joined_df = X_with_id.join(y_with_id, on='id', how='inner')

    # Remove the 'id' column
    joined_df = joined_df.drop('id')

    # Combine numerical features into a vector column using VectorAssembler
    vector_col = "features"
    assembler = VectorAssembler(inputCols=joined_df.columns, outputCol=vector_col)
    df_vector = assembler.transform(joined_df).select(vector_col)

    # Calculate the correlation matrix
    correlation_matrix = Correlation.corr(df_vector, vector_col, corr_mod).head()[0]

    # Converting DenseMatrix to a NumPy array and then to a Python list
    correlation_array = correlation_matrix.toArray().tolist()

    # Creating an RDD and converting it to a Spark DataFrame
    correlation_rdd = df_vector.rdd.context.parallelize(correlation_array)
    correlation_df = correlation_rdd.toDF(joined_df.columns)

    return correlation_df, joined_df


def heatmap(correlation_data, joined_df):
    # Converting Row Objects to 2D Lists
    correlation_values = [list(row) for row in correlation_data]

    # Heat mapping of correlations
    plt.figure(figsize=(12, 10))
    plt.imshow(correlation_values, cmap='coolwarm', interpolation='nearest')
    plt.colorbar()

    # Setting the scale and labels for the x and y axes
    plt.xticks(ticks=range(len(joined_df.columns)), labels=joined_df.columns, rotation=90)
    plt.yticks(ticks=range(len(joined_df.columns)), labels=joined_df.columns)
    plt.title('Correlation Heatmap for Numeric Features')
    # Show Heat Map
    plt.show()

def feature_correlation(correlation_data, joined_df):
    # Extract correlation data from Diabetes_binary column (last column)
    diabetes_corr = [row['Diabetes_binary'] for row in correlation_data]

    # Extracting other feature names as transverse labels
    features = [col for col in joined_df.columns if col != 'Diabetes_binary']

    # Plotting line graphs
    plt.figure(figsize=(12, 6))
    plt.plot(features, diabetes_corr[:-1], marker='o', linestyle='-', color='skyblue')  # Excluding the last item (Diabetes_binary's own correlation with itself)

    # Setting up chart labels
    plt.xlabel('Features')
    plt.ylabel('Correlation with Diabetes_binary')
    plt.title('Correlation of Features with Diabetes_binary')
    plt.xticks(rotation=45, ha='right')  
    plt.grid(True)

    # Show Chart
    plt.tight_layout()
    plt.show()

def select_corr_features(correlation_df):
    # Extract the index of the Diabetes_binary column
    diabetes_idx = correlation_df.columns.index('Diabetes_binary')

    # Create a list of tuples (feature_name, correlation_value) excluding 'Diabetes_binary'
    correlation_list = [
        (correlation_df.columns[i], correlation_df.collect()[i][diabetes_idx])
        for i in range(len(correlation_df.columns))
        if correlation_df.columns[i] != 'Diabetes_binary'
    ]

    # Sort the list by correlation value in descending order and select the top 4
    top_4_features = sorted(correlation_list, key=lambda x: abs(x[1]), reverse=True)[:4]

    # Extract the feature names of the top 4 features
    selected_columns = [feature[0] for feature in top_4_features]

    # Print the top 4 features with their correlation values
    print("Top 4 features with highest correlation with Diabetes_binary:")
    for feature, value in top_4_features:
        print(f"{feature}: {value}")

    # Add 'Diabetes_binary' to the selected columns
    selected_columns.append('Diabetes_binary')

    return selected_columns


def pca_filter(columns_to_include, X_pyspark, number_of_selected_features, add_composite_features=True):
    assembler = VectorAssembler(inputCols=columns_to_include, outputCol='features')
    data = assembler.transform(X_pyspark)

    pca = PCA(k=number_of_selected_features, inputCol='features', outputCol='pca_features')
    pca_model = pca.fit(data)
    data_with_pca = pca_model.transform(data)

    components = pca_model.pc.toArray()  # Matrix of principal components

    contributions = [sum(abs(components[i])) for i in range(len(components[0]))]

    contribution_indices = sorted(range(len(contributions)), key=lambda i: contributions[i], reverse=True)[:number_of_selected_features]
    selected_features = [columns_to_include[i] for i in contribution_indices]
    merged_features = selected_features 
    if add_composite_features:
        merged_features += ['Diet', 'cardiovascular', 'unhealthy_behavior', 'healthcare']
    print("Selected features based on PCA contributions:", selected_features)

    X_pyspark = X_pyspark.select(*merged_features)
    return X_pyspark


def plot_absolute_contributions(feature_loadings_df, num_components):
    selected_components = feature_loadings_df.iloc[:, :num_components]

    # Calculate the sum of the absolute values of contributions for each feature
    absolute_contributions = selected_components.abs().sum(axis=1)

    # Create a bar plot
    plt.figure(figsize=(12, 6))
    absolute_contributions.plot(kind='bar', color='royalblue')
    plt.title(f'Sum of Absolute Contributions to First {num_components} Principal Components')
    plt.xlabel('Features')
    plt.ylabel('Sum of Absolute Contributions')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y')
    plt.tight_layout()
    plt.show()

def plot_explained_variance(explained_variance_pd, explained_variance):
    # Set up the figure and axes for two subplots
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Scree plot (Explained Variance for each component)
    line1, = ax1.plot(range(1, len(explained_variance) + 1), explained_variance, 
                      marker='o', linestyle='-', color='b', label='Explained Variance Ratio')  # Add label for the legend
    ax1.set_title('Explained Variance and Cumulative Explained Variance')
    ax1.set_xlabel('Number of Principal Components')
    ax1.set_ylabel('Explained Variance Ratio', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.grid(True)

    ax2 = ax1.twinx()
    # Create a second y-axis for cumulative variance
    line2, = ax2.plot(explained_variance_pd["Component"], explained_variance_pd["Cumulative Variance"], 
                      marker='o', color='orange', linestyle='--', label='Cumulative Explained Variance')  # Add label for the legend
    ax2.set_ylabel('Cumulative Explained Variance', color='orange')
    ax2.tick_params(axis='y', labelcolor='orange')

    # Adding a horizontal line at 75% cumulative variance for reference
    line3 = ax2.axhline(y=0.75, color='red', linestyle='--', label='75% Cumulative Variance')

    # Combine legends
    handles = [line1, line2, line3]  # List of handles from both axes
    labels = [line1.get_label(), line2.get_label(), '75% Cumulative Variance']  # Corresponding labels

    # Create a single legend
    ax1.legend(handles, labels, loc='center right')

    plt.xticks(range(1, len(explained_variance_pd) + 1))  # Set x-ticks to be the component numbers
    plt.tight_layout()
    plt.show()

def plot_principal_components(pca_features):
    """
    Plots a 2D scatter plot of the first two principal components.

    Parameters:
        pca_features (ndarray): A NumPy array containing the PCA features, where each column corresponds to a principal component.
    """
    fig = plt.figure(figsize=(12, 8))  # Adjust the size based on your preference
    ax = fig.add_subplot(111)

    # Scatter plot of the first two principal components
    ax.scatter(pca_features[:, 0], pca_features[:, 1], c='b', marker='o', alpha=0.3)

    # Set the limits for the axes using Python's built-in min and max functions
    ax.set_xlim(np.min(pca_features[:, 0]) - 1, np.max(pca_features[:, 0]) + 1)
    ax.set_ylim(np.min(pca_features[:, 1]) - 1, np.max(pca_features[:, 1]) + 1)

    # Set labels and title
    ax.set_xlabel('Principal Component 1')
    ax.set_ylabel('Principal Component 2')
    ax.set_title('2D Scatter Plot of First Two Principal Components')

    plt.show()

def plot_principal_components_3d(pca_features):
    """
    Plots a 3D scatter plot of the first three principal components.

    Parameters:
        pca_features (ndarray): A NumPy array containing the PCA features, where each column corresponds to a principal component.
    """
    fig = plt.figure(figsize=(20, 16))
    ax = fig.add_subplot(111, projection='3d')

    # Scatter plot of the first three principal components
    ax.scatter(pca_features[:, 0], pca_features[:, 1], pca_features[:, 2], c='b', marker='o', alpha=0.3)

    # Set the limits for the axes
    ax.set_xlim(-2, 1)  # Adjust these values based on your PCA results
    ax.set_ylim(-1, 2)  # Adjust these values based on your PCA results
    ax.set_zlim(-2, 1.5)

    # Set labels and title
    ax.set_xlabel('Principal Component 1')
    ax.set_ylabel('Principal Component 2')
    ax.set_zlabel('Principal Component 3')
    ax.set_title('3D Scatter Plot of First Three Principal Components')

    plt.show()

def plot_pca_with_clusters(pca_features, clusters):
    """
    Plots a 3D scatter plot of the first three principal components with cluster colors.

    Parameters:
        pca_features (ndarray): A NumPy array containing the PCA features, where each column corresponds to a principal component.
        clusters (ndarray): A NumPy array containing the cluster labels for each data point.
    """
    fig = plt.figure(figsize=(20, 16))
    ax = fig.add_subplot(111, projection='3d')

    # Use clusters to color the scatter plot
    scatter = ax.scatter(pca_features[:, 0], pca_features[:, 1], pca_features[:, 2], 
                         c=clusters, cmap='tab10', marker='o', alpha=0.8)

    # Set axis limits (adjust based on your data)
    ax.set_xlim(-2, 1)
    ax.set_ylim(-1, 2)
    ax.set_zlim(-2, 1.5)

    # Set labels
    ax.set_xlabel('Principal Component 1')
    ax.set_ylabel('Principal Component 2')
    ax.set_zlabel('Principal Component 3')
    ax.set_title('3D Scatter Plot of First Three Principal Components with KMeans Clusters')

    # Add color bar to indicate different clusters
    plt.colorbar(scatter)

    # Show the plot
    plt.show()

def plot_eigenvalues(ev):
    """
    Plots a bar plot of eigenvalues from factor analysis.

    Parameters:
        ev (ndarray): A NumPy array containing the eigenvalues.
    """
    plt.figure(figsize=(10, 6))
    plt.bar(range(1, len(ev) + 1), ev, color='skyblue')
    plt.title('Eigenvalues from Factor Analysis')
    plt.xlabel('Factors')
    plt.ylabel('Eigenvalues')
    plt.xticks(range(1, len(ev) + 1))  # Set x-ticks to factor numbers
    plt.axhline(y=1, color='r', linestyle='--', label='Eigenvalue = 1')
    plt.legend()
    plt.grid(axis='y')
    plt.show


def plot_models_results(results, models_name_mapping, datasets=None, message=None):
    datasets = datasets if datasets else list(results.keys())
    message = message if message else 'for Different Models Across Datasets'
    metrics = ['accuracy', 'f1', 'recall', 'precision']

    bar_width = 0.1 
    x = np.arange(len(datasets))  

    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(15, 10))
    axes = axes.flatten()  

    for i, metric in enumerate(metrics):
        all_values = []
        for model_name in models_name_mapping.keys():  
            values = [results[dataset][model_name][metric] for dataset in datasets]
            all_values.extend(values)  
            
            # Create bars for the current metric and model
            axes[i].bar(x + (list(models_name_mapping.keys()).index(model_name) * bar_width), 
                        values, 
                        width=bar_width, 
                        label=models_name_mapping[model_name])

        min_value = np.min(all_values)
        max_value = np.max(all_values)
        axes[i].set_ylim(min_value * 0.95, max_value * 1.05)  # Add padding around limits

        axes[i].set_xlabel('Datasets')
        axes[i].set_ylabel(metric.capitalize())
        axes[i].set_title(f'{metric.capitalize()} {message}')
        
        axes[i].set_xticks(x + bar_width * (len(results[datasets[0]].keys()) + 5) / 2)
        axes[i].set_xticklabels(datasets, rotation=0, ha="right")
        axes[i].legend()
        axes[i].legend(framealpha=0.3)  
    plt.tight_layout()
    plt.show()


def plot_all_results(all_results, models, metrics, feature_methods, model_names_mapping, sampling_methods):
    fig, axes = plt.subplots(nrows=len(metrics), ncols=len(feature_methods), figsize=(24, 20))
    axes = axes.flatten()

    bar_width = 0.25
    x = np.arange(len(models))  

    for i, metric in enumerate(metrics):
        for j, feature_method in enumerate(feature_methods):
            all_values = []
            for k, sampling_method in enumerate(sampling_methods):
                results = all_results[sampling_method]
                metric_values = [results[feature_method][model][metric] for model in models]
                all_values.extend(metric_values)
                axes[i * len(feature_methods) + j].bar(
                    x + k * bar_width, metric_values, width=bar_width, label=sampling_method
                )
            
            ax = axes[i * len(feature_methods) + j]
            ax.set_xticks(x + bar_width) 
            ax.set_xticklabels([model_names_mapping[model] for model in models], rotation=45, ha='right')
            ax.set_xlabel("Models")
            ax.set_ylabel(metric.capitalize())
            ax.set_title(f'{metric.capitalize()} - {feature_method.replace("_", " ").title()}')
            ax.legend(title="Sampling Strategy", framealpha=0.5)  
            
            min_value = np.min(all_values)
            max_value = np.max(all_values)
            ax.set_ylim(min_value * 0.95, max_value * 1.05)

    plt.tight_layout()
    plt.show()

def balance_dataset(X_pyspark, y_pyspark):
    # Add an ID column to align X_pyspark and y_pyspark
    X_pyspark_with_id = X_pyspark.withColumn("id", monotonically_increasing_id())
    y_pyspark_with_id = y_pyspark.withColumn("id", monotonically_increasing_id())

    # Join X_pyspark and y_pyspark on the 'id' column
    combined_df = X_pyspark_with_id.join(y_pyspark_with_id, "id").drop("id")

    # Display original class distribution
    original_counts = combined_df.groupBy('Diabetes_binary').count().collect()
    count_0 = next(count['count'] for count in original_counts if count['Diabetes_binary'] == 0)
    count_1 = next(count['count'] for count in original_counts if count['Diabetes_binary'] == 1)
    print(f"Original class distribution: class 0 - {count_0}; class 1 - {count_1}")

    # Undersample the majority class (0s) to match the minority class (1s)
    majority_class = combined_df.filter(col('Diabetes_binary') == 0)
    undersampled_majority = majority_class.sample(withReplacement=False, fraction=count_1 / count_0, seed=42)

    # Combine the undersampled majority class with the minority class (1s)
    minority_class = combined_df.filter(col('Diabetes_binary') == 1)
    balanced_df = undersampled_majority.union(minority_class)

    # Split the combined DataFrame back into X and y
    balanced_X = balanced_df.select(X_pyspark.columns)
    balanced_y = balanced_df.select("Diabetes_binary")

    # Display the balanced class distribution
    balanced_counts = balanced_y.groupBy('Diabetes_binary').count().collect()
    count_0 = next(count['count'] for count in balanced_counts if count['Diabetes_binary'] == 0)
    count_1 = next(count['count'] for count in balanced_counts if count['Diabetes_binary'] == 1)
    print(f"Balanced class distribution: class 0 - {count_0}; class 1 - {count_1}")

    return balanced_X, balanced_y


