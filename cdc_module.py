import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import pyspark
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

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

    # Get feature names with Diabetes_binary correlation greater than 0.2
    selected_columns = [
        correlation_df.columns[i] for i in range(len(correlation_df.columns))
        if correlation_df.collect()[i][diabetes_idx] > 0.2 and correlation_df.columns[i] != 'Diabetes_binary'
    ]

    # Print correlation indicators greater than 0.2
    print("Features with correlation greater than 0.2 with Diabetes_binary:")
    print(selected_columns)
    selected_columns.append('Diabetes_binary')

    return selected_columns