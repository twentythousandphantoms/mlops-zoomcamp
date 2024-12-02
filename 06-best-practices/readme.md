

HW: https://github.com/DataTalksClub/mlops-zoomcamp/blob/main/cohorts/2024/06-best-practices/homework.md

```

conda deactivate
conda activate exp-tracking-env

pipenv install --dev pytest
pipenv install s3fs
pip install s3fs

pip uninstall boto3
pip install boto3 botocore==1.35.36

```

```
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

aws configure

AWS Access Key ID: dummy
AWS Secret Access Key: dummy
Default region name: us-east-1
Default output format: json

```
docker-compose up -d
aws s3 mb s3://nyc-duration --endpoint-url http://localhost:4566
aws s3 ls --endpoint-url http://localhost:4566
```

```
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet
aws s3 cp yellow_tripdata_2023-03.parquet s3://nyc-duration/in/2023-03.parquet --endpoint-url http://localhost:4566
aws s3 ls s3://nyc-duration/in/ --endpoint-url http://localhost:4566
```


```
export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
export OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"

export S3_ENDPOINT_URL="http://localhost:4566"

python script.py 2023 3

pytest tests
```
