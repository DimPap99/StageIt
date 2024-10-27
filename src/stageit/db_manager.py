from utils import DB_Enum
from databases.postgresql_db import PostgresDB
from databases.sqlite_db import SQLiteDB
from databases.base_db import BaseDB
class DB_Manager():
    
    
    def __init__(self, conn_str:str, db:BaseDB, db_type:DB_Enum, schema:str='public'):
        self.conn_str = conn_str
        self.schema = schema
        self.db_type = db_type
        self.db = db