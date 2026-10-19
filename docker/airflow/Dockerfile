FROM apache/airflow:3.3.2-python3.11

RUN pip install --no-cache-dir "apache-airflow==3.3.2" "duckdb==1.5.6" "boto3" "pyyaml"

RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL postgres')"
