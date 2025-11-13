import yaml
from pydantic import BaseModel, ConfigDict, computed_field,SecretStr,Field
from typing import Literal,Union,Dict
from lake.util.logger import logger
import os
class PgCnn(BaseModel):
    host: str = os.getenv("PG_HOST",'127.0.0.1')
    port: int = int(os.getenv("PG_PORT",'9000'))
    username: SecretStr = SecretStr(os.getenv("PG_USERNAME",'PG_USERNAME'))
    password: SecretStr = SecretStr(os.getenv("PG_SECRET",'PG_SECRET'))
    database: str = os.getenv("PG_DATABASE",'PG_DATABASE')
    schema: str = os.getenv("PG_SCHEMA",'public')
    lake_alias: str = "lake"
    @computed_field
    @property
    def url(self) -> str:
        return f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

class StorageCnn(BaseModel):
    host: str = os.getenv("STORAGE_HOST",'127.0.0.1')
    port: int = int(os.getenv("STORAGE_PORT",'9000'))
    access_key: SecretStr = SecretStr(os.getenv("STORAGE_ACCESS",'minio'))
    secret: SecretStr = SecretStr(os.getenv("STORAGE_SECRET",'password'))
    region: str = os.getenv("STORAGE_REGION",'STORAGE_REGION')
    secure: bool = bool(eval(os.getenv("STORAGE_SECURE",'False')))
    scope: str = os.getenv("STORAGE_SCOPE",'bucketname')
    style: Literal["path","vhs"] = os.getenv("STORAGE_STYLE",'path')
    lake_alias: str = "lake"
    @computed_field
    @property
    def get_address(self) -> str:
        return f"{self.host}:{self.port}"
    @computed_field
    @property
    def http_url(self) -> str:
        protocol = "https" if self.secure else "http"
        return f"{protocol}://{self.host}:{self.port}"
    @computed_field
    @property
    def aws_url(self) -> str:
        if self.style == 'vhs':
            return f"https://{self.access_key}:{self.secret}@{self.scope}.s3.{self.region}.amazonaws.com"
        return f"https://{self.access_key}:{self.secret}@s3.{self.region}.amazonaws.com/{self.scope}"
    
class ELKCnn(BaseModel):
    host: str = os.getenv("ELK_HOST",'127.0.0.1')
    port: int = int(os.getenv("ELK_PORT",'9000'))
    username: SecretStr = SecretStr(os.getenv("ELK_USERNAME",'ELK_USERNAME'))
    password: SecretStr = SecretStr(os.getenv("ELK_PASSWORD",'ELK_PASSWORD'))
    secure: bool = bool(eval(os.getenv("STORAGE_SECURE",'False')))
    scope:str = "default"
    @computed_field
    @property
    def url(self) -> str:
        if self.secure:
            return f"https://{self.host}"
        else:
            return f"http://{self.host}:{self.port}"

class KibanaCnn(BaseModel):
    host: str = os.getenv("KIBANA_HOST",'127.0.0.1')
    port: int = int(os.getenv("KIBANA_PORT",'9000'))
    username: SecretStr = SecretStr(os.getenv("KIBANA_USERNAME",'KIBANA_USERNAME'))
    password: SecretStr = SecretStr(os.getenv("KIBANA_PASSWORD",'KIBANA_PASSWORD'))
    secure: bool = bool(eval(os.getenv("KIBANA_SECURE",'False')))
    @computed_field
    @property
    def url(self) -> str:
        if self.secure:
            return f"https://{self.host}"
        else:
            return f"http://{self.host}:{self.port}"
        

class BrokerCnn(BaseModel):
    host: str = os.getenv("BROKER_HOST",'127.0.0.1')
    port: int = int(os.getenv("BROKER_PORT",'9000'))
    username: SecretStr = SecretStr(os.getenv("BROKER_USERNAME",'BROKER_USERNAME'))
    password: SecretStr = SecretStr(os.getenv("BROKER_PASSWORD",'BROKER_PASSWORD'))
    type: str = []
    @computed_field
    @property
    def url(self) -> str:
        if self.type in ['rabbit','rabbitmq','amqp']:
            return f'amqp://{self.username.get_secret_value()}:{self.password.get_secret_value()}@{self.host}:{self.port}'
        elif self.type == 'redis':
            return f'redis://{self.username.get_secret_value()}:{self.password.get_secret_value()}@{self.host}:{self.port}'
class SRC(BaseModel):
    storage: Dict[str, StorageCnn] = {}
    postgres: Dict[str, PgCnn] = {}
    
class DEST(BaseModel):
    catalog: PgCnn
    storage: StorageCnn

class Lake(BaseModel):
    DEST: DEST
    SRC: SRC

    
class BrokerCnn(BaseModel):
    host: str = "127.0.0.1"
    port: int = 5432
    ingest_topics: list = []
    ingest_table: str
    group_id: str = ""
    batch_size: int = 1000
    @computed_field
    @property
    def url(self) -> str:
        return f"{self.host}:{self.port}"
    
class Configs(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    Lake: Lake
    BrokerCnn: BrokerCnn
    def __init__(self, config_file_path:str) -> None:
        self.load_config(config_file_path)

    def load_config(self,config_file_path:str):
        with open(config_file_path) as yaml_in:
            yaml_object = yaml.safe_load(yaml_in)

        # for key in self.model_fields:
        #     if key not in list(yaml_object.keys()):
        #         logger.warning(f"no {key} found in config file! (initiating from env values)")
        #         yaml_object[key] = {}
        super().__init__(**yaml_object)

