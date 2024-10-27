import datetime
import numpy as np
import pandas as pd


TURN_LOGGING_ON=False
LOGGING_DIR = None

PSQL_TYPE_MAPPING = {
    np.int8: "SMALLINT",
    np.int16: "SMALLINT",
    np.int32: "INTEGER",
    np.int64: "BIGINT",
    np.float32: "REAL",
    np.float64: "DOUBLE PRECISION",
    np.bool_: "BOOLEAN",
    np.datetime64: "TIMESTAMP",
    datetime.date: "DATE",
    datetime.datetime: "TIMESTAMP",
    pd.Timestamp: "TIMESTAMP",
    pd.Timedelta: "INTERVAL",
    np.object_: "TEXT",
    np.str_: "TEXT",
    np.bytes_: "BYTEA"
}

SQLITE_TYPE_MAPPING = {
    np.int8: "INTEGER",
    np.int16: "INTEGER",
    np.int32: "INTEGER",
    np.int64: "INTEGER",
    np.float32: "REAL",
    np.float64: "REAL",
    np.bool_: "INTEGER",  
    np.datetime64: "TEXT",
    datetime.date: "TEXT",
    datetime.datetime: "TEXT",
    pd.Timestamp: "TEXT",
    np.object_: "TEXT",
    np.str_: "TEXT",
    np.bytes_: "BLOB"
}


