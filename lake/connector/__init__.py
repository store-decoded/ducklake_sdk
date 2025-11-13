import importlib
import sys
from typing import Literal, cast,Union
from . import *

def load_lake(servicer:str,config_path:str):
    try:
        module = importlib.import_module(f".{servicer}", package=__name__)
        return module.Connector(config_path)
    except ImportError:
        print(f"Error: Module '{servicer}' not found (please create your module as\
              (./lake/connector/{servicer}.py)) to become enabled!")
        sys.exit(1)
