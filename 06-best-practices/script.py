#!/usr/bin/env python
# coding: utf-8

import os
import sys
import pickle
import pandas as pd


def prepare_data(df, categorical):
    """
    Applies transformations to the DataFrame.

    Parameters:
    - df: Input DataFrame
    - categorical: List of categorical columns

    Returns:
    - Transformed DataFrame
    """
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    # Filter outliers
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    # Process categorical columns
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


def get_input_path(year, month):
    """
    Returns the input file path based on environment variables or defaults.

    Parameters:
    - year: Year of the dataset
    - month: Month of the dataset

    Returns:
    - Input file path as a string
    """
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    """
    Returns the output file path based on environment variables or defaults.

    Parameters:
    - year: Year of the dataset
    - month: Month of the dataset

    Returns:
    - Output file path as a string
    """
    default_output_pattern = 's3://nyc-duration-prediction/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)


def read_data(filename, categorical):
    """
    Reads and preprocesses the input Parquet file.

    Parameters:
    - filename: Path or URL of the Parquet file
    - categorical: List of categorical columns to process

    Returns:
    - DataFrame with processed data
    """
    print(f"Reading data from {filename}...")

    # Check if S3_ENDPOINT_URL is set for Localstack S3
    s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
    if s3_endpoint_url:
        print(f"Using Localstack S3 endpoint: {s3_endpoint_url}")
        options = {'client_kwargs': {'endpoint_url': s3_endpoint_url}}
        df = pd.read_parquet(filename, storage_options=options)
    else:
        print("No S3 endpoint specified. Using default method.")
        df = pd.read_parquet(filename)

    print("Data read successfully. Preparing data...")
    return prepare_data(df, categorical)


def save_data(df, filename):
    print(f"Saving data to {filename}...")
    s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
    options = {
        'client_kwargs': {'endpoint_url': s3_endpoint_url},
        'key': 'dummy',
        'secret': 'dummy'
    }
    df.to_parquet(filename, engine='pyarrow', index=False, storage_options=options)
    print("Data saved successfully.")


def main(year, month):
    """
    Main function to perform predictions and save results.

    Parameters:
    - year: Year of the dataset
    - month: Month of the dataset
    """
    print(f"Starting the process for {year}-{month:02d}...")
    
    # Get input and output paths
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")

    print("Loading the model...")
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)
    print("Model loaded successfully.")

    categorical = ['PULocationID', 'DOLocationID']

    # Read and preprocess the data
    df = read_data(input_file, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    # Prepare features and make predictions
    print("Preparing features for prediction...")
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    print("Making predictions...")
    y_pred = lr.predict(X_val)

    # Print the mean predicted duration
    mean_duration = y_pred.mean()
    print(f"Predicted mean duration: {mean_duration}")

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred
    save_data(df_result, output_file)
    print("Process completed successfully.")


# Main block
if __name__ == "__main__":
    # Parse command-line arguments
    year = int(sys.argv[1])
    month = int(sys.argv[2])

    # Run main function
    main(year, month)
