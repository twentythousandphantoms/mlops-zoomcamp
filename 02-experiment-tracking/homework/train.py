import os
import pickle
import click
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("RandomForestRegressor")

def load_pickle(filename: str):
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)


@click.command()
@click.option(
    "--data_path",
    default="./output",
    help="Location where the processed NYC taxi trip data was saved"
)
def run_train(data_path: str):
    print("Loading training and validation data...")
    try:
        X_train, y_train = load_pickle(os.path.join(data_path, "train.pkl"))
        X_val, y_val = load_pickle(os.path.join(data_path, "val.pkl"))
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    print("Starting MLflow autologging...")
    mlflow.sklearn.autolog()

    print("Training the model with MLflow tracking...")
    with mlflow.start_run():
        rf = RandomForestRegressor(max_depth=10, random_state=0)
        rf.fit(X_train, y_train)

        print("Predicting on validation data...")
        y_pred = rf.predict(X_val)

        print("Calculating RMSE...")
        rmse = mean_squared_error(y_val, y_pred, squared=False)
        print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")


if __name__ == '__main__':
    run_train()
