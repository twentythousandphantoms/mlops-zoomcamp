FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

WORKDIR /app

COPY script.py /app/script.py

# Create the result directory
RUN mkdir -p /app/result

# Install dependencies
RUN pip install pandas pyarrow scikit-learn==1.5.0

# Define the command to run the script
ENTRYPOINT ["python", "script.py"]
