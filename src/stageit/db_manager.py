from .utils import DB_Enum
from .databases.base_db import BaseDB

class DB_Manager():

    def __init__(self, db:BaseDB):
        self._db:BaseDB = db
        
    async def connect(self):
        await self._db.connect()

    async def create_table(self, table_name, columns):
        await self._db.create_table(table_name, columns)
        
    async def drop_table(self, table_name):
        await self._db.drop_table(table_name)
        
    async def get_existing_columns(self, table_name):
        return await self._db.get_columns(table_name)

    async def add_missing_columns(self, table_name, new_columns):
        await self._db.add_columns(table_name, new_columns)

    async def insert_data(self, table_name, data, columns):
        await self._db.insert(table_name, data, columns)

    async def close(self):
        await self._db.close()
    
    async def __aenter__(self):
        """Handles the asynchronous setup."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Handles the asynchronous teardown."""
        await self.close()