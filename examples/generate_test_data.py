import numpy as np
import random
import datetime


db_url = "sqlite:///testdb.sqlite"
 # Define a function to generate a value based on the column's data type
def generate_value(col_name):
    base_type = col_name.split('_')[0]  # Get the base type (e.g., "integer" from "integer_1")
    if base_type == "integer":
        return np.random.randint(0, 100000)
    elif base_type == "float":
        return np.random.rand() * 100
    elif base_type == "boolean":
        return random.choice([True, False])
    elif base_type == "date":
        return datetime.date(2000, 1, 1) + datetime.timedelta(days=np.random.randint(0, 365))
    elif base_type == "timestamp":
        return datetime.datetime(2000, 1, 1, 12, 0, 0) + datetime.timedelta(hours=np.random.randint(0, 8760))
    elif base_type == "text":
        return random.choice(['apple', 'banana', 'cherry', 'date', 'fig', 'grape', None])
    elif base_type == "mixed":
        return random.choice([42, 'mixed_data', 3.14, 'random_string', None])


def generate_csv_like_test_data(num_rows=50, max_cols=10):
    # Seed for reproducibility
    np.random.seed(42)
    random.seed(42)

    # Define all possible data types
    possible_data_types = ["integer", "float", "boolean", "date", "timestamp", "text", "mixed"]

    # Randomly select data types for up to max_cols columns
    
    selected_data_types = random.choices(possible_data_types, k=max_cols)
    
    # Create column names with suffixes (_1, _2, etc.) based on selected data types
    selected_columns = [f"{dtype}_{i+1}" for i, dtype in enumerate(selected_data_types)]

    # Initialize an empty list to hold rows
    data = []
    
   
    # Generate rows with a consistent number of columns
    for _ in range(num_rows):
        num_cols = random.randint(5, max_cols)
        row = [generate_value(selected_columns[i]) for i in range(num_cols)]
        data.append(row)
    return selected_columns, data

# Generate the CSV-like test data
column_names, test_data = generate_csv_like_test_data(50, max_cols=10)
max_y = max([len(r) for r in test_data])
np.array(test_data, dtype=object)
# Display the column names and a sample of the data
print("Column Names:", column_names)
print("First 5 Rows of Data:")
for row in test_data[:5]:
    print(row)
