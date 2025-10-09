import yaml
from pydantic import BaseModel, ConfigDict, computed_field,SecretStr
from typing import Literal,Union
from typing import Optional, Union
class PgCnn(BaseModel):
    host: str = "127.0.0.1"
    port: int = 5432
    username: SecretStr = ""
    password: SecretStr = ""
    database: str = "postgres"
    lake_alias: str = "pg_default"
    @computed_field
    @property
    def url(self) -> str:
        return f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

class StorageCnn(BaseModel):
    host: str = "127.0.0.1"
    port: int = 9000
    access_key: SecretStr = ""
    secret: SecretStr = ""
    scope: str = "bucketname"
    region: str = "us-east-1"
    secure: bool = False
    style: str = Literal["path","vhs"]
    lake_alias: str = "s3_default"
    @computed_field
    @property
    def get_address(self) -> str:
        protocol = "https" if self.secure else "http"
        return f"{protocol}://{self.host}:{self.port}"
    @computed_field
    @property
    def minio_url(self) -> str:
        protocol = "https" if self.secure else "http"
        if self.style == 'vhs':
            return f"{protocol}://{self.scope}.{self.host}:{self.port}"
        return f"{protocol}://{self.host}:{self.port}/{self.scope}"
    @computed_field
    @property
    def aws_url(self) -> str:
        if self.style == 'vhs':
            return f"https://{self.access_key}:{self.secret}@{self.scope}.s3.{self.region}.amazonaws.com"
        return f"https://{self.access_key}:{self.secret}@s3.{self.region}.amazonaws.com/{self.scope}"
    
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
    

class SRC(BaseModel):
    stream: BrokerCnn
    storage: Optional[StorageCnn] = None
    postgres: Optional[PgCnn] = None
    
class DEST(BaseModel):
    catalog: PgCnn
    storage: StorageCnn

class Configs(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    SRC: SRC
    DEST: DEST
    def __init__(self, config_file_path:str) -> None:
        self.load_config(config_file_path)

    def load_config(self,config_file_path:str):
        with open(config_file_path) as yaml_in:
            yaml_object = yaml.safe_load(yaml_in)
        super().__init__(**yaml_object)

