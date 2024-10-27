from abc import ABC, abstractmethod
from typing import Union
import numpy as np
import pandas as pd


class BaseDB(ABC):
    def __init__(self, db_url:str, schema:str=None):
        super().__init__()
        self.db_url = db_url
        self.schema = schema
    
    @abstractmethod
    async def connect(self):
        """Connects to the database
        """
        pass    
    
    @abstractmethod
    async def get_columns(self, table_name:str):
        """Returns the missing columns from the table

        Args:
            table_name (str): Table name
        """
        pass
    
    @abstractmethod
    async def create_table(self, table_name:str, columns: dict = None):
        """

        Args:
            table_name (str): Table name
        """
        pass
    
    @abstractmethod
    async def drop_table(self, table_name:str):
        """

        Args:
            table_name (str): Table name
        """
        pass
    
    
    @abstractmethod
    async def add_columns(self, table_name:str, new_columns:dict):
        """Adds the missing columns to the table

        Args:
            table_name (str): Table name
        """
        
    @abstractmethod
    async def insert(self, table_name, data: Union[np.ndarray, pd.DataFrame], columns_and_types):
        """Inserts data into the table specified

        Args:
            table_name (_type_): Table Name
            data (Union[np.ndarray, pd.DataFrame]): The data to be inserted. (np array or dataframe)
        """
        pass
    
    @abstractmethod
    async def close(self):
        """Close the database connection."""
        pass