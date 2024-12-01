FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    libffi-dev libpq-dev
RUN pip install psycopg2-binary pandas requests
RUN pip install evidently


COPY quantile_metric_calculation.py /app/quantile_metric_calculation.py
WORKDIR /app

CMD ["python", "quantile_metric_calculation.py"]
