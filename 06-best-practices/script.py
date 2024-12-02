#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd


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
    df = pd.read_parquet(filename)
    
    print("Calculating trip duration...")
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    print("Filtering outliers in duration...")
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    print("Processing categorical columns...")
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    print("Data reading and preprocessing completed.")
    return df


def main(year, month):
    """
    Main function to perform predictions and save results.

    Parameters:
    - year: Year of the dataset
    - month: Month of the dataset
    """
    print(f"Starting the process for {year}-{month:02d}...")
    
    input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'output/taxi_type=yellow_year={year:04d}_month={month:02d}.parquet'

    print("Loading the model...")
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)
    print("Model loaded successfully.")

    categorical = ['PULocationID', 'DOLocationID']

    # Read and preprocess the data
    print("Reading and preprocessing data...")
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

    # Save results to Parquet
    print(f"Saving results to {output_file}...")
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred
    df_result.to_parquet(output_file, engine='pyarrow', index=False)
    print("Process completed successfully.")


# Main block
if __name__ == "__main__":
    # Parse command-line arguments
    year = int(sys.argv[1])
    month = int(sys.argv[2])

    # Run main function
    main(year, month)
