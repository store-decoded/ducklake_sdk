import sys
import boto3
import duckdb
import psycopg2
from typing import List, Optional
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from botocore.exceptions import ClientError, ConnectTimeoutError
from lake.util.conf_loader import Configs,StorageCnn,PgCnn
import time
from lake.util.logger import logger
from duckdb import CatalogException
from typing import Literal, cast,Union
from duckdb import IOException

class DuckLakeManager(Configs):
    pg_catalog:str = None
    s3_source_create_command: str = None
    healthy: bool = None
    duckdb_connection: duckdb.DuckDBPyConnection = None
    def __init__(self,config_path):
        super(DuckLakeManager,self).__init__(config_path)
        self.duckdb_connection = duckdb.connect()
        try:
            self._attach()
            result = self.duckdb_connection.execute(f"""
                SELECT tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname = 'public';
                """).fetchall()
            print("Available DataSrc:",[x[0] for x in result if not x[0].startswith('ducklake')])
            if len(result) == 0:
               raise CatalogException
            logger.info(f"attached existing ducklake {self.Lake.DEST.catalog.lake_alias} with {len(result)} tables")
        except CatalogException:
            logger.warning(f"catalog Not found! (Creating {self.Lake.DEST.catalog.lake_alias}...)")
            self._connectivity_assessment()
            installation_status = self.__install_duckdb_extensions()
            if installation_status is not None:
                sys.exit(1)


    def _get_dest_storage_secret(self):
        try:
            return (
                f"create secret {self.Lake.DEST.storage.lake_alias} (type s3, "
                + f"key_id '{self.Lake.DEST.storage.access_key.get_secret_value()}', "
                + f"secret '{self.Lake.DEST.storage.secret.get_secret_value()}', "
                + f"endpoint '{self.Lake.DEST.storage.host}:{self.Lake.DEST.storage.port}', "
                + f"scope 's3://{self.Lake.DEST.storage.scope}',"
                + f"use_ssl {self.Lake.DEST.storage.secure}, "
                + f"url_style '{self.Lake.DEST.storage.style}'"
                ");"
            )
        except Exception as fail:
            logger.critical(f"cannot register DataPath Lake secret {fail}")
            return './resources/asset/tmp.db'
    def _get_dest_catalog_definition(self):
        try:
            return (
                "postgres:"
                + f"dbname={self.Lake.DEST.catalog.database} "
                + f"host={self.Lake.DEST.catalog.host} "
                + f"port={self.Lake.DEST.catalog.port} "
                + f"user={self.Lake.DEST.catalog.username.get_secret_value()} "
                + f"password={self.Lake.DEST.catalog.password.get_secret_value()} "
            )
        except Exception as fail:
            logger.critical(f"cannot register catalog definition {fail}")
            return ''
    def _get_chsql_extra_definition(self):
        try:
            return (
                f"CREATE SECRET extra_http_headers (type HTTP, "
                + "EXTRA_HTTP_HEADERS MAP{"
                +   f"'X-ClickHouse-User': 'data.bimebazar.com',"
                +   f"'X-ClickHouse-Key': '5up3r53CUR3D'"
                + "});"
            )
        except Exception as fail:
            logger.critical(f"cannot register catalog definition {fail}")
            return ''
    @staticmethod
    def _get_src_pg_secret(lake_alias:str,pg_cfg:PgCnn):
        try:
            return (
                f"create secret {lake_alias}_secret (type postgres, "
                + f"host '{pg_cfg.host}', "
                + f"port {pg_cfg.port}  , "
                + f"database '{pg_cfg.database}', "
                + f"user '{pg_cfg.username.get_secret_value()}',"
                + f"password '{pg_cfg.password.get_secret_value()}' "
                + ");"
            )
        except Exception as fail:
            logger.critical(f"cannot register {lake_alias} {fail}")
            return 'select 1;'
    @staticmethod
    def _get_src_s3_secret(lake_alias:str,storage_cfg:StorageCnn):
        try:
            return (
                f"create secret {lake_alias}_secret (type s3, "
                + f"key_id '{storage_cfg.access_key.get_secret_value()}', "
                + f"secret '{storage_cfg.secret.get_secret_value()}', "
                + f"endpoint '{storage_cfg.get_address}', "
                + f"scope 's3://{storage_cfg.scope}', "
                + f"use_ssl {storage_cfg.secure}, "
                + f"url_style '{storage_cfg.style}'"
                ");"
            )
        except Exception as fail:
            logger.critical(f"cannot register {lake_alias} {fail}")
            return 'select 1;'

    def _attach(self):
        try:
            self.duckdb_connection.execute(self._get_dest_storage_secret())
        except Exception as fail:
            logger.error(f'ducklake (DataPath) storage has not been registered ! {fail}')
        
        if self.Lake.SRC.storage:
            
            for lake_alias,storage_cfg in self.Lake.SRC.storage.items():
                logger.info(f"register minio instance {lake_alias} on scope {storage_cfg.scope}")
                try:
                    self.duckdb_connection.execute(self._get_src_s3_secret(lake_alias,storage_cfg))
                except Exception as fail:
                    logger.error(f"Failed to register new s3 instance {lake_alias} on scope {storage_cfg.scope} ")
        if self.Lake.SRC.postgres:
            logger.info(f"registering postgres source {list(self.Lake.SRC.postgres.keys())}")
            for lake_alias,pg_cfg in self.Lake.SRC.postgres.items():
                self.duckdb_connection.execute(self._get_src_pg_secret(lake_alias,pg_cfg))
                attach_src_pg_command = f"ATTACH 'dbname={pg_cfg.database}' AS {lake_alias} (TYPE postgres, SECRET {lake_alias}_secret);"
                self.duckdb_connection.execute(attach_src_pg_command)
        logger.info(f"registering core 'DATA LAKE' as {self.Lake.DEST.catalog.lake_alias}")
        attach_lake_command = f"ATTACH 'ducklake:{self._get_dest_catalog_definition()}' AS {self.Lake.DEST.catalog.lake_alias} (DATA_PATH 's3://{self.Lake.DEST.storage.scope}');"
        try:
            print(attach_lake_command)
            register_lake = self.duckdb_connection.execute(attach_lake_command).fetchall()
            logger.info(register_lake)
        except IOException:
            logger.error(f'error registering core data lake! (installing extenstions)')
            raise CatalogException
             


    def _connectivity_assessment(self):
        s3_object = self.Lake.DEST.storage
        pg_object = self.Lake.DEST.catalog
        try:
            logger.info(f"checking connectivity for source s3")
            s3_client = boto3.client(
                "s3",
                endpoint_url=s3_object.http_url,
                aws_access_key_id=s3_object.access_key.get_secret_value(),
                aws_secret_access_key=s3_object.secret.get_secret_value()
            )
        except ClientError as e:
            logger.error(f'failed to assert connection to s3://{s3_object.scope}| {e}')
            time.sleep(3)
            self._connectivity_assessment()
        try:
            bucket_data = s3_client.head_bucket(Bucket=s3_object.scope)
            assert bucket_data['ResponseMetadata']['HTTPStatusCode'] == 200
            logger.debug(f'bucket {s3_object.scope} already exist (no action needed)')
        except ClientError as e:
            logger.error(f'cannot find bucket with name {s3_object.scope} (creating...)')
            try:
                s3_client.create_bucket(Bucket=s3_object.scope)
            except ClientError as e:
                logger.error(f'there is a problem with defined bucket name or \
                             client doesnt seem to have right permissions to create new bucket\
                                -> {s3_object.scope} {e}')
                time.sleep(3)
                self._connectivity_assessment()
        logger.info("s3 connectivity test successfull")
        pg_conn = None
        try:
            logger.info(f"checking connectivity for catalog data-store({pg_object})")
            pg_conn = psycopg2.connect(
                host=pg_object.host,
                port=pg_object.port,
                user=pg_object.username.get_secret_value(),
                password=pg_object.password.get_secret_value(),
                dbname='postgres',
            )
            pg_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with pg_conn.cursor() as cursor:
                db_name = pg_object.database
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)
                )
                exists = cursor.fetchone()
                if not exists:
                    logger.info(f"database '{db_name}' not found. Creating it now...")
                    cursor.execute(f'CREATE DATABASE "{db_name}"')
                    logger.info(f"database '{db_name}' created successfully.")
                else:
                    logger.info(
                        f"database '{db_name}' already exists. No action needed."
                    )
        except psycopg2.Error as e:
            logger.error(f"postgreSQL error: {e}")
            time.sleep(3)
            self._connectivity_assessment()
        finally:
            if pg_conn:
                pg_conn.close()
        logger.info("catalog connectivity test successfull")

    def __install_duckdb_extensions(
        self, extensions: List = ["ducklake", "postgres", "httpfs","excel"]
    ) -> Optional[Exception]:

        for extension_name in extensions:
            try:
                self.duckdb_connection.sql(f"INSTALL {extension_name};")
                
                self.duckdb_connection.sql(f"LOAD {extension_name};")
                logger.info(f"{extension_name} installed and loaded successfully.")
            except duckdb.HTTPException as e:
                logger.error(
                    f"extension {extension_name} not found or you might have connectivity issues:\n{e}"
                )
                return e
            except Exception as e:
                logger.error(
                    f"during installation of {extension_name} an unexpected error has occoured: {e}"
                )
                return e

    def retrive_snapshot(self,commit_type:Literal['tables_inserted_into','tables_deleted_from'],table_name:str):
        """Use time travel to investigate what happened."""
        print("Investigation: What Happened?")

        snapshots = self.duckdb_connection.execute(
            """
            SELECT snapshot_id, snapshot_time, changes
            FROM ducklake_snapshots('lake')
            ORDER BY snapshot_id DESC
            LIMIT 10;
        """
        ).fetchall()
        
        logger.info("Analyzing recent changes...")

        # Find the deletion snapshot
        deletion_snapshot = None
        previous_snapshot = None
        # tables_inserted_into

        for i, (snap_id, snap_time, changes) in enumerate(snapshots):
            print(snap_id,snap_time,list(changes.keys()))
            if commit_type in list(changes.keys()):
                target_snapshot = snap_id
                if i + 1 < len(snapshots):
                    previous_snapshot = snapshots[i + 1][0]
                break

        if target_snapshot:
            logger.warning(f"Found deletion in snapshot {deletion_snapshot}")
            logger.info(f"   Previous good snapshot: {previous_snapshot}")
            if previous_snapshot:
                logger.info("Data that was deleted:")
                prev_data_state = self.duckdb_connection.execute(
                    f"""
                    SELECT * FROM {table_name} AT (VERSION => {previous_snapshot})
                """
                ).fetchdf()
                print(prev_data_state)
        return prev_data_state