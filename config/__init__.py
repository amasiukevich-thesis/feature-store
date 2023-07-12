import yaml
from datetime import datetime, timedelta
import os

CONFIG_PATH = os.environ.get("CONFIG_PATH")

def parse_yaml():
    yaml_dict = {}
    with open(CONFIG_PATH, "r") as stream:
        try:
            yaml_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print("ERROR")
            print(exc)

    return yaml_dict

yaml_dict = parse_yaml()

STUMP_FOLDER = yaml_dict.get("STUMP_FOLDER")
DB_FILENAME = yaml_dict.get("DB_FILENAME")
DUCKDB_FILENAME = yaml_dict.get("DUCKDB_FILENAME")
CONNECTION_STRING = yaml_dict.get("CONNECTION_STRING")
CREATOR_SCRIPT_FILENAME = yaml_dict.get("CREATOR_SCRIPT_FILENAME")

STUMP_PATH_RAW = yaml_dict.get("STUMP_PATH_RAW")
STUMP_PATH = yaml_dict.get("STUMP_PATH")


DATABASE_FOLDER = yaml_dict.get("DATABASE_FOLDER")
RAW_DATA_FOLDER = yaml_dict.get("RAW_DATA_FOLDER")

URL = yaml_dict.get("URL")
DATE_COL = yaml_dict.get("DATE_COL")
TIME_AT_REFRESH = yaml_dict.get("TIME_AT_REFRESH") or datetime.strftime(
    datetime.now() + timedelta(minutes=2), "%H:%M"
)
