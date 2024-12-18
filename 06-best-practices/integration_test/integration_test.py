import os
import pandas as pd
from datetime import datetime
import subprocess

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

# Create the test dataframe
data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
]
columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)

# Set the input file path and S3 options
input_file = "s3://nyc-duration/in/2023-01.parquet"
options = {
    'key': 'dummy',  # Dummy Access Key
    'secret': 'dummy',  # Dummy Secret Key
    'client_kwargs': {
        'endpoint_url': os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')
    }
}

# Save the dataframe to S3
df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)

print("Test data saved to S3.")

# Run the script for January 2023
print("Running script.py...")
subprocess.run(["python", "script.py", "2023", "1"], check=True)

# Read the output from S3
output_file = "s3://nyc-duration/out/2023-01.parquet"
df_output = pd.read_parquet(output_file, storage_options=options)
print("Output data read from S3.")

# Verify the sum of predicted durations
sum_predicted_durations = df_output['predicted_duration'].sum()
print(f"Sum of predicted durations: {sum_predicted_durations}")
