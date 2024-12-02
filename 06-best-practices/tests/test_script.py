import pandas as pd
from datetime import datetime
from script import prepare_data

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def test_prepare_data():
    # Input Data
    data = [
        (None, None, dt(1, 1), dt(1, 10)),      
        (1, 1, dt(1, 2), dt(1, 10)),           
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),  
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]
    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)

    # Expected Output
    expected_data = [
        (-1, -1, dt(1, 1), dt(1, 10), 9.0),
        (1, 1, dt(1, 2), dt(1, 10), 8.0),
    ]
    expected_columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']
    expected_df = pd.DataFrame(expected_data, columns=expected_columns)
    expected_df[['PULocationID', 'DOLocationID']] = expected_df[['PULocationID', 'DOLocationID']].astype('str')

    # Test the prepare_data function
    categorical = ['PULocationID', 'DOLocationID']
    actual_df = prepare_data(df, categorical)

    # Assert that the actual DataFrame matches the expected DataFrame
    assert actual_df.to_dict(orient='records') == expected_df.to_dict(orient='records')
