# DuckLake (SDK) 🧰 

A super quick, no-nonsense guide to wiring up your lake.

basic infrastructure for a datalake operating on top of (ducklake,duckdb,postgress,kafka,minio[s3])



## Getting Started
there are some backing services you need to run in order to use primary functionallity:
```bash
cd infrastructure/
docker compose up 
```
this command should make these services availabe using default environment variables:

- kafka [http://localhost:9092] 
- kafka-ui [http://localhost:28080]
- minio [s3://localhost:9000]
- minio-console [http://localhost:9001]
- postgres [postgres://localhost:5432]
- pg-admin [http://localhost:18888]

each of these sevices will initiate by reading these environement variables:
- postgres:
    - PG_ADVERT_PORT: default=9000
    - PG_USER: default='pgadmin'
    - PG_PASSWORD: default='password'
    - PG_VOLUME_DIR: default='./pg_data'
- pgadmin:
    - PGADMIN_ADVERT_PORT: default=18888
    - PGADMIN_USER: default='pgadmin@email.com'
    - PGADMIN_DEFAULT_PASSWORD: default='password'
    - PGADMIN_VOLUME_DIR: default='./pgadmin_data'
- minio:
    - MINIO_ADVERT_PORT: default=9000
    - MINIOCONSOLE_ADVERT_PORT: default=9001
    - MINIO_ACCESS_KEY: default='minio'
    - MINIO_SECRET_KEY: default='password'
    - MINIO_VOLUME_DIR: default='./minio_data'
- kafka:
    - KAFKA_BROKER_PORT: default=9092
    - KAFKA_CONTROLLER_PORT: default=19292
    - KAFKA_INTERNAL_PORT: default=29092
    - KAFKA_HOST_ADDRESS: default=172.16.3.246 (must set to your exposed IP)
    - KAFKA_VOLUME_DIR: default='./kafka_data'
- kafka-ui
    - KAFKA_UI_PORT: default=18080



You point the project at:
- where your lake should live (DEST), and
- where your data is coming from (SRC),

by editing one file: `./resources/config.yml`.

That’s it. The tooling reads this config and does the rest.


---

## TL;DR

1) Open `./resources/config.yml`  
2) Set your DEST (where the lake lives)  
3) Pick a SRC (what you’re reading from) / or leave it empty if you like!   
4) Install deps:

The config file (resources/config.yml) 

| There are two top-level keys: DEST and SRC. 
 
##### $`\textcolor{yellow}{\text{DEST => where your lake “lives”}}`$  
DEST has two parts: catalog and storage. 


## catalog
| This is your relational database that stores the lake’s [catalog tables](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database) \
including (schemas, table definitions, etc..)  for more information read \
[catalog tables spec](https://ducklake.select/docs/stable/specification/tables/overview)


         

## storage 
This is where the actual data files from [compatible sources](https://ducklake.select/docs/stable/duckdb/usage/choosing_storage) will go (the “data-path” of your lake). \
ducklake requires you to specify a data location while
[Connecting to data-path](https://ducklake.select/docs/stable/duckdb/usage/connecting).

     
In short:

* catalog = metadata database (e.g., Postgres) 
* storage = object store for data (e.g., MinIO/S3)    


##### $`\textcolor{yellow}{\text{SRC => what you’re reading from}}`$  

SRC defines the upstream source you want to pull from to build the lake. You can choose one (or more) of: 

* stream (e.g., Kafka)
* storage (e.g., MinIO/S3 full of parquet)
* postgres (a relational DB)
    

you should be able to define all of them and attach to each one as demand by running ('use {lake_alias}') inside your custom deploy() definition runtime


![hld](resources/asset/lake.png)


## Example config (just a sketch)

```yml
SRC:
  stream:
    host: 127.0.0.1 # kafka bootstrap host that you want to ingest
    port: 9092 # kafka broker port
    ingest_topics: # the topic names to ingest data from (multiple)
      - test_topic 
    ingest_table: kafka_src # the name of table where observed messages from topic will be written in
    group_id: ducklake # consumer group name
  storage:
    host: 127.0.0.1 # data included s3fs host that you want to read from
    port: 9000 # data included s3fs port
    scope: bucket_name # the bucket you will have access to by defining s3://{scope}/my_parquet_files_2025-05-*.parquet
    secure: false # http=false | https=true
    region: us-east-1 # matters only if you choose style=vhs
    style: path # you can choose either vhs(for aws) or path 
    access_key: minio # s3fs access key
    secret: password # s3fs secret key
    lake_alias: my_src_s3 # the alias to use connection (using {lake_alias}; select * from ...;) 
  postgres:
    host: 127.0.0.1 # data included postgres that you want to read from
    port: 5432 # data included postgres port
    database: postgres # target database to invoke queries on
    username: pgadmin # postgres username
    password: password # postgres password
    lake_alias: my_src_pg # the alias to use this connection (using {lake_alias}; select * from ...;) 
DEST: # all fields similar to SRC
  catalog:
    host: 127.0.0.1
    port: 5432
    database: postgres
    username: pgadmin
    password: password
    lake_alias: lake # the alias to use when you want to query on core datalake (using {lake_alias}; select * from ...;) 
  storage:
    host: 127.0.0.1
    port: 9000
    scope: destination
    secure: false
    region: us-east-1
    style: path
    access_key: minio
    secret: password
    lake_alias: dest_s3_secret # this alias is only used to ceate an s3 secret for ducklake initiation (the lake will then be able to resolve scope using read_parquet('s3://{scope}/my_file.parquet');) 
```
 
$`\textcolor{green}{\text{Note}}`$ \
    You don’t have to fill all of SRC. Use the one(s) you need.
    Keep secrets out of git. Environment variables or a secrets manager are your friends.
     


next navigate to the root of project:
```bash
uv pip install -e .
```



if you dont have uv installed follow the steps below:

- https://docs.astral.sh/uv/getting-started/installation

After a successful installation, you should be able to invoke the CLI: 

```bash
lake --help 
```

You can get attached to the stream you have defined to ingest incoming data by
```bash
lake attach --config resources/config.yml 
Aliases: -c for --config 
```

## Usage


To learn how to utilize the `Connector` class, navigate to the example file located at `lake/connector/personal.py`. Below is a sample implementation:

```python
class Connector(DuckLakeManager):
    def __init__(self,config_path):
        super(Connector,self).__init__(config_path)
        
    def deploy(self):
        # connect to your ducklake (the data and tables you have defined inside lake will be accessible to query)
        self.duckdb_connection.execute(f"use {self.DEST.catalog.lake_alias};")
        read_from_ducklake = "select * from kafka_content;" # the value defined in stream.ingest_table 
        result = self.duckdb_connection.execute(read_from_ducklake)
        print(result.df())

        # connect to your postgres src (the data and tables from your SRC:postgres will be accessible to query)
        self.duckdb_connection.execute(f"use {self.SRC.postgres.lake_alias};")
        read_from_src_pg = "select * from public.my_table_in_src limit 100 ;"
        result = self.duckdb_connection.execute(read_from_src_pg)
        print(result.df())

        # connect to your storage src (no need to call use {alias} command since ducklake automatically detects from scope)
        read_from_src_storage = f"select count(request_id) as num_requests,remote_ip as address from read_parquet('s3://{self.SRC.storage.scope}/website_logs.parquet') \
            group by remote_ip;"
        result = self.duckdb_connection.execute(read_from_src_storage)
        print(result.df())

        # create any plot inside this code-block and return it
        df = result.df()
        df.plot(kind = 'bar', x = 'address', y = 'num_requests')
        plt.title(__file__.split('/')[-1])
        plt.xlabel("ip_address")
        plt.ylabel("requests")
        plt.grid()
        # return current figure after modification
        return plt.gcf()

```



Members of your data analysis team can customize the deploy method to return a Matplotlib plot, which can be used to register their own dashboard on the Dashboards page. This codebase is designed to make the Python module you create under ./lake/pages/{the_name}.py available when executing the following command.

```bash
lake serve --config resources/config.yml
Aliases: -c for --config 
```



Project status: This project is under active development. Please report bugs or issues this repo or hashempourian.a@gmail.com.
