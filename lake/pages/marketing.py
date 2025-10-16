
from lake.connector.core import DuckLakeManager
from lake.util.logger import logger
import matplotlib.pyplot as plt
import numpy as np

class Connector(DuckLakeManager):
    def __init__(self,config_path):
        super(Connector,self).__init__(config_path)
        
    def deploy(self):
        # connect to your ducklake
        # self.duckdb_connection.execute(f"use {self.DEST.catalog.lake_alias};")
        # read_from_ducklake = "select * from kafka_content;" # the value defined in stream.ingest_table 
        # result = self.duckdb_connection.execute(read_from_ducklake)
        # print(result.df())

        # # connect to your postgres src
        # self.duckdb_connection.execute(f"use {self.SRC.postgres.lake_alias};")
        # read_from_src_pg = "select * from public.my_table_in_src limit 100 ;"
        # result = self.duckdb_connection.execute(read_from_src_pg)
        # print(result.df())

        # connect to your storage src (no need to call use {alias} command since ducklake automatically detects from scope)
        read_from_src_storage = f"select count(request_id) as num_requests,remote_ip as address from read_parquet('s3://flowtrack/logs_2024-09-20T00-20.parquet') \
            group by remote_ip;"
        result = self.duckdb_connection.execute(read_from_src_storage)
        # print(result.df())

        # create any plot inside this code-block and return it
        df = result.df()
        df.plot(kind = 'bar', x = 'address', y = 'num_requests')
        plt.title(__file__.split('/')[-1])
        plt.xlabel("ip_address")
        plt.ylabel("requests")
        plt.grid()
        return plt.gcf()
