import asyncpg
from abc import ABC
from typing import Union
import pandas as pd
import numpy as np
from io import StringIO
from base_db import BaseDB

class PostgresDB(BaseDB):
    def __init__(self, db_url: str, schema: str = 'public'):
        super().__init__(db_url, schema)
        self.conn = None

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

    async def insert(self, table_name: str, df: pd.DataFrame):
        """Insert data into the PostgreSQL table, adding columns dynamically if necessary."""
        existing_columns = await self.get_columns(table_name)
        new_columns = {col: str(df[col].dtype) for col in df.columns if col not in existing_columns}
        if new_columns:
            await self.add_columns(table_name, new_columns)

        output = StringIO()
        df.to_csv(output, index=False, header=False)
        output.seek(0)
        copy_query = f"COPY {self.schema}.{table_name} ({', '.join(df.columns)}) FROM STDIN WITH CSV"
        await self.conn.copy_records_to_table(table_name, records=output)

    async def close(self):
        """Close the PostgreSQL database connection."""
        if self.conn:
            await self.conn.close()

    def _map_dtype_to_sql(self, dtype, db_type):
        """Map data types to PostgreSQL SQL types."""
        if dtype == 'float64':
            return 'DOUBLE PRECISION'
        elif dtype == 'int64':
            return 'BIGINT'
        elif dtype == 'bool':
            return 'BOOLEAN'
        else:
            return 'TEXT'
