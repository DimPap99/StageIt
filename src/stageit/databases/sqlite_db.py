import aiosqlite
from abc import ABC
from typing import Union
import pandas as pd
import numpy as np
from .base_db import BaseDB

class SQLiteDB(BaseDB):
    def __init__(self, db_url: str, schema: str = None):
        super().__init__(db_url, schema)
        self.conn:aiosqlite.Connection = None

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
        alter_table_commands = "\n".join(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type};"
        for column_name, sql_type in new_columns.items()
    )

        # Execute the concatenated command as a single transaction
        await self.conn.executescript(alter_table_commands)
        await self.conn.commit()

    
    async def create_table(self, table_name: str, columns: dict = None):
        """Creates a table in SQLite with the specified columns, or only an ID column if no columns are provided."""
        if not columns:
            columns = {"id": "INTEGER PRIMARY KEY AUTOINCREMENT"}  # Default to ID-only table

        # Build the CREATE TABLE statement
        columns_def = ", ".join(f"{col_name} {col_type}" for col_name, col_type in columns.items())
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def});"

        # Execute the query
        async with self.conn.cursor() as cursor:
            await cursor.execute(create_table_query)
        await self.conn.commit()
        print(f"Table '{table_name}' created in SQLite database with only an ID column.")

    async def drop_table(self, table_name: str):
        """Drops a table in SQLite."""
        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
        async with self.conn.cursor() as cursor:
            await cursor.execute(drop_table_query)
        await self.conn.commit()
        print(f"Table '{table_name}' dropped in SQLite database.")


    async def insert(self, table_name, data: pd.DataFrame, columns_and_types: dict):
        """Insert data into the SQLite table, adding columns dynamically if necessary."""
        
        # Step 1: Get existing columns
        existing_columns = await self.get_columns(table_name)
        
        # Step 2: Identify and add new columns if necessary
        new_columns = {col: columns_and_types[col] for col in data.columns if col not in existing_columns}
        if new_columns:
            await self.add_columns(table_name, new_columns)

        # Step 3: Replace NaN, NaT, and Timestamp values with compatible types
        data = data.map(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) and pd.notna(x) 
                            else None if pd.isna(x) 
                            else x)

        # Step 4: Prepare insert statement with placeholders
        placeholders = ', '.join(['?'] * len(data.columns))
        query = f"INSERT INTO {table_name} ({', '.join(data.columns)}) VALUES ({placeholders})"

        # Step 5: Execute the batch insert
        # Convert DataFrame to list of tuples compatible with `executemany`
        await self.conn.executemany(query, data.itertuples(index=False, name=None))
        await self.conn.commit()



    async def close(self):
        """Close the SQLite database connection."""
        if self.conn:
            await self.conn.close()

    