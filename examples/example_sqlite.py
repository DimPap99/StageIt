
import asyncio
import sys
from pathlib import Path

# Add the parent directory of 'src' to the system path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Now you can import directly from 'src'
from src.stageit.databases.sqlite_db import SQLiteDB
from src.stageit.stager import Stager
from src.stageit.schema_manager import SchemaManager
from src.stageit.db_manager import DB_Manager
from src.stageit.utils import DB_Enum

from generate_test_data import generate_csv_like_test_data


column_names, test_data = generate_csv_like_test_data(50, max_cols=10)
DROP_FRIST = True
async def main(test_data):
    
    db_url = "testdb.sqlite"
    
    stager = Stager(db_url, DB_Enum.SQLITE, None)
    
    await stager.stage_data_async(test_data, drop_first=DROP_FRIST)
    # Instantiate the SQLiteDB class

    # # Connect to the database
    # await db.connect()
    # # Perform other operations here, such as inserting or querying data
    
    # # Close the database connection
    # await db.close()

# Run the main function
asyncio.run(main(test_data))
