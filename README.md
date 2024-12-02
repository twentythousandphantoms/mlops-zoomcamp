# mlops-zoomcamp

```
conda activate exp-tracking-env
mlflow ui --backend-store-uri sqlite:///mlflow.db
mlflow server \
    --backend-store-uri sqlite:///mlflow.db \
    --default-artifact-root ./artifacts
```