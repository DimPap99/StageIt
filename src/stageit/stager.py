from .db_manager import DB_Manager
from .schema_manager import SchemaManager
from .databases.postgresql_db import PostgresDB
from .databases.sqlite_db import SQLiteDB
from .databases.base_db import BaseDB
from .utils import DB_Enum
import pandas as pd

class Stager():
    
    def __init__(self, conn_url, db_type:DB_Enum, schema="public"):
        if db_type == DB_Enum.SQLITE:
            db = SQLiteDB(conn_url, schema=None)
        else:
            db = PostgresDB(conn_url, schema=schema)
        self._db_manager = DB_Manager(db)
        self._schema_manager = SchemaManager(db_type)
    
    
    async def ensure_schema(self, table_name: str, inferred_types: dict[str, str]):
        """
        Ensures that the table schema is compatible with the data. 
        Adds missing columns as needed.
        
        Parameters:
        - table_name (str): The name of the table in the database.
        - inferred_types (Dict[str, str]): A dictionary of column names and their inferred SQL types.
        """
        existing_columns = set(await self._db_manager.get_existing_columns(table_name))
        
        # Identify missing columns
        missing_columns = {
            col_name: col_type
            for col_name, col_type in inferred_types.items()
            if col_name not in existing_columns
        }
        
        if missing_columns:
            await self._db_manager.add_missing_columns(table_name, missing_columns)
        else:
            print(f"Schema for table '{table_name}' is already up-to-date.")


    async def stage_data_async(self, data, table_name="staging", schema=None, drop_first=False):
        
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data, columns=None)            
            data.columns = [f"column_{i}" for i in range(data.shape[1])]
        columns = self._schema_manager.infer_types(data)
        async with self._db_manager as db:
            if drop_first:
                await self._db_manager.drop_table(table_name)
            #create table if it doesnt exist
            await self._db_manager.create_table(table_name, None)
            # Perform data staging or any async database operation
            await db.insert_data(table_name, data, columns)
            
            
    def stage_data():
        #        asyncio.run(self.async_ingest_data(table_name, df))
        pass
        
        
        
    
    