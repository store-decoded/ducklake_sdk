
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
        read_from_src_storage = f"select Suburb,avg(Median_House_Price_AUD) as Median_House_Price_AUD  from read_parquet('s3://data-source/suburb_data.parquet') GROUP BY Suburb LIMIT 50;"
        result = self.duckdb_connection.execute(read_from_src_storage)
        df = result.df()
        df.sort_values('Median_House_Price_AUD',inplace=True)
        df.plot(kind = 'bar', x = 'Suburb', y = 'Median_House_Price_AUD')
        plt.title(__file__.split('/')[-1])
        plt.xlabel("Suburb")
        plt.ylabel("Prices")
        plt.grid()

        return plt.gcf()
