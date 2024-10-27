import aiosqlite
from abc import ABC
from typing import Union
import pandas as pd
import numpy as np
from base_db import BaseDB

class SQLiteDB(BaseDB):
    def __init__(self, db_url: str, schema: str = None):
        super().__init__(db_url, schema)
        self.conn = None

    async def connect(self):
        """Establish a connection to SQLite."""
        self.conn = await aiosqlite.connect(self.db_url)

    async def get_columns(self, table_name: str):
        """Fetch existing columns in the specified SQLite table."""
        query = f"PRAGMA table_info({table_name});"
        async with self.conn.execute(query) as cursor:
            return {row[1] for row in await cursor.fetchall()}

    async def add_columns(self, table_name: str, new_columns: dict):
        """Add missing columns to the SQLite table based on incoming data."""
        for column_name, dtype in new_columns.items():
            sql_type = self._map_dtype_to_sql(dtype, "sqlite")
            alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type};"
            await self.conn.execute(alter_table_query)
        await self.conn.commit()

    async def insert(self, table_name: str, df: pd.DataFrame):
        """Insert data into the SQLite table, adding columns dynamically if necessary."""
        existing_columns = await self.get_columns(table_name)
        new_columns = {col: str(df[col].dtype) for col in df.columns if col not in existing_columns}
        if new_columns:
            await self.add_columns(table_name, new_columns)

        placeholders = ', '.join(['?'] * len(df.columns))
        query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
        async with self.conn.executemany(query, df.itertuples(index=False, name=None)):
            await self.conn.commit()

    async def close(self):
        """Close the SQLite database connection."""
        if self.conn:
            await self.conn.close()

    def _map_dtype_to_sql(self, dtype, db_type):
        """Map data types to SQLite SQL types."""
        if dtype == 'float64':
            return 'REAL'
        elif dtype == 'int64':
            return 'INTEGER'
        elif dtype == 'bool':
            return 'INTEGER'  # SQLite lacks BOOLEAN type, so we use INTEGER
        else:
            return 'TEXT'
