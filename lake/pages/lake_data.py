
from lake.connector.core import DuckLakeManager
from lake.util.logger import logger
import matplotlib.pyplot as plt
import numpy as np

class Connector(DuckLakeManager):
    def __init__(self,config_path):
        super(Connector,self).__init__(config_path)
        
    def deploy(self):
        # connect to your ducklake


        # # connect to your postgres src
        # self.duckdb_connection.execute(f"use {self.SRC.postgres.lake_alias};")
        # read_from_src_pg = "select * from public.my_table_in_src limit 100 ;"
        # result = self.duckdb_connection.execute(read_from_src_pg)
        # print(result.df())
        read_from_ducklake = f"select * from lake.awesome_table;" 
        result = self.duckdb_connection.execute(read_from_ducklake).fetch_df()
        print(result.shape)
        result.sort_values('region',inplace=True)

        result.plot(kind = 'bar', x = 'region', y = 'num_of_resinets')
        plt.title(__file__.split('/')[-1])
        plt.xlabel("region")
        plt.ylabel("num_residents")
        plt.grid()
        return plt.gcf()
