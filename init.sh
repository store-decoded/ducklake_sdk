#!/bin/bash

mkdir -p infrastructure/{kafka_data,minio_data,pg_data,pgadmin_data}
mkdir -p infrastructure/airflow/{logs,plugins}
mkdir -p resources/local_lake
docker network create lake_net
sudo chown 50000:root -R infrastructure/airflow
sudo chmod 777 -R infrastructure/airflow