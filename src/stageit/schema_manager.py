import numpy as np
import pandas as pd
from typing import Union
from config import PSQL_TYPE_MAPPING, SQLITE_TYPE_MAPPING
from utils import DBEnum

class SchemaManager():        
    def __init__(self, db_type:DBEnum):
        if db_type == DBEnum.PSQL:
            self.type_mapping = PSQL_TYPE_MAPPING
        else:
            self.type_mapping = SQLITE_TYPE_MAPPING
    
    def infer_from_dataframe(self, data:pd.DataFrame) ->dict:
        """Infer Database types from a dataframe
        It is assumed that if someone uses a dataframe they have already provided custom column names.

        Args:
            data (pd.DataFrame): raw dataframe data

        Returns:
            dict: _description_
        """
        return {col: self.type_mapping.get(type(data[col].iloc[0]), "TEXT") for col in data.columns}
    
    
    
    def infer_from_np_array(self, data:np.ndarray, col_names:list) -> dict:
        """Infer Database types from numpy array

        Args:
            data (np.ndarray): raw numpy array data
            cum_col_names (list, optional): Custom names for the columns. Defaults to None.

        Raises:
            ValueError: Raised if the data are not tabular. E.G. dimensions >= 3

        Returns:
            dict:  A map between the the columns and Database Types
        """
        if data.ndim == 1:
            # Single-dimensional array case
            return {col_names[0] : self.type_mapping.get(data.dtype.type, "TEXT")}
        elif data.ndim == 2:
            # Multi-dimensional array (assumed to represent columns)                
            return {
                # Infer from dtype of each column
                col_names[i] : self.type_mapping.get(data[:, i].dtype.type, "TEXT")  
                for i in range(data.shape[1])
            }
        else:
            raise ValueError("Unsupported array dimension. Provide 1D or 2D array.")
        
    def infer_types(self, data: Union[np.ndarray, pd.DataFrame, list, tuple]) -> dict:
        """Infers the column types of the data provided.

        Args:
            data (Union[np.ndarray, pd.DataFrame, list, tuple]): The data for which we will infer their db data types

        Raises:
            ValueError: Raised if the instance of the date is not np.ndarray, pd.DataFrame, list, tuple

        Returns:
            dict: A map between the the columns and Database Types
        """
        if isinstance(pd.DataFrame):
            return self.infer_from_dataframe(data)
        elif isinstance(np.ndarray):
            return self.infer_from_np_array(data)
        elif isinstance(list) or isinstance(tuple):
            return self.infer_from_np_array(np.array(data, dtype=object))
        else:
            raise ValueError("Unsupported data instance.")