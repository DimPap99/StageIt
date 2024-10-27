import asyncpg
from abc import ABC
from typing import Union
import pandas as pd
import numpy as np
from io import StringIO
from .base_db import BaseDB

class PostgresDB(BaseDB):
    def __init__(self, db_url: str, schema: str = 'public'):
        super().__init__(db_url, schema)
        self.conn:asyncpg.Connection = None

    async def connect(self):
        """Establish a connection to PostgreSQL."""
        self.conn = await asyncpg.connect(self.db_url)

    async def get_columns(self, table_name: str):
        """Fetch existing columns in the specified PostgreSQL table."""
        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{self.schema}' AND table_name = '{table_name}';
        """
        rows = await self.conn.fetch(query)
        return {row['column_name'] for row in rows}

    async def add_columns(self, table_name: str, new_columns: dict):
        """Add missing columns to the PostgreSQL table based on incoming data."""
        for column_name, dtype in new_columns.items():
            sql_type = self._map_dtype_to_sql(dtype, "postgresql")
            alter_table_query = f"ALTER TABLE {self.schema}.{table_name} ADD COLUMN {column_name} {sql_type};"
            await self.conn.execute(alter_table_query)

    async def insert(self, table_name, data: pd.DataFrame, columns_and_types: dict):
        """Insert data into the PostgreSQL table, adding columns dynamically if necessary."""
        
        # Step 1: Get existing columns
        existing_columns = await self.get_columns(table_name)
        
        # Step 2: Identify and add new columns if necessary
        new_columns = set(data.columns) - set(existing_columns)
        if new_columns:
            new_cols_and_types = {k: columns_and_types[k] for k in new_columns if k in columns_and_types}
            await self.add_columns(table_name, new_cols_and_types)

        # Step 3: Prepare CSV format for insertion
        output = StringIO()
        data.to_csv(output, index=False, header=False)
        output.seek(0)

        # Step 4: Use `copy_from` with the CSV string data
        copy_query = f"COPY {self.schema}.{table_name} ({', '.join(data.columns)}) FROM STDIN WITH CSV"
        await self.conn.copy_from(output, copy_query)


    async def close(self):
        """Close the PostgreSQL database connection."""
        if self.conn:
            await self.conn.close()

    async def create_table(self, table_name: str, columns: dict = None):
        """Creates a table in PostgreSQL with the specified columns, or only an ID column if no columns are provided."""
        if not columns:
            columns = {"id": "SERIAL"}  # Default to ID-only table

        # Build the CREATE TABLE statement
        columns_def = ", ".join(f"{col_name} {col_type}" for col_name, col_type in columns.items())
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def});"

        # Execute the query
        await self.conn.execute(create_table_query)
        print(f"Table '{table_name}' created in PostgreSQL database with only an ID column.")
    
    
    async def drop_table(self, table_name: str):
        """Drops a table in PostgreSQL."""
        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
        await self.conn.execute(drop_table_query)
        print(f"Table '{table_name}' dropped in PostgreSQL database.")

