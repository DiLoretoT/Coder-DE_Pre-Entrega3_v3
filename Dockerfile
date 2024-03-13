FROM apache/airflow:2.7.3-python3.9

RUN pip install --no-cache-dir pandas==2.2.1 SQLAlchemy==1.4.51 pathlib requests==2.25.1 psycopg2-binary redshift_connector==2.1.0
