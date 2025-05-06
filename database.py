import sqlite3
import pandas as pd
import polars as pl
import numpy as np
import json
from collections import defaultdict
import datetime
import re
import os
import yaml
from io import StringIO # Import StringIO
import utils
import plotly.graph_objects as go
from plotly.subplots import make_subplots



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
pl.Config.set_float_precision(2) 
strptime            = datetime.datetime.strptime


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Define constants outside of expressions
P_ATM               = 1.01325
CPF                 = 0.0002

# WARNING: database needs to be manually initialized
database_file     = "operation.db"

table_names       = [
    "delivery",
    "customer",
    "restock"
]


db_table_columns = {
    "delivery"   : [
        "delivery_id",
        "customer_id",
        "delivery_route",
        "transport_plate_number",
        "arrival_timestamp",
        "pre_buffer_pressure",
        "delivery_stand_meter",
        "delivery_pressure",
        "delivery_temperature",
        "post_buffer_pressure",
        "transport_bank_pressure",
    ],
    "customer"   : [
        "customer_id",
        "customer_name",
        "customer_address",
        "subscription_type",
        "subscription_start",
        "liter_weight_capacity",
        "minimum_monthly_volume",
        "buffer_count",
        "applied_price"
    ],
    "restock"    : [
        "restock_id",
        "restock_date",
        "transport_plate_number",
        "restock_volume",
        "gas_station_address"
    ]

}


# Map from English day names to Indonesian day names
day_name_map = {
    "Monday"    : "Senin",
    "Tuesday"   : "Selasa",
    "Wednesday" : "Rabu",
    "Thursday"  : "Kamis",
    "Friday"    : "Jumat",
    "Saturday"  : "Sabtu",
    "Sunday"    : "Minggu"
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def create_table_with_schema(db_file, table_name, schema):
    """
    Creates a SQLite table with specified column names and data types.

    Args:
        db_file (str): The path to the SQLite database file.
    """

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Construct the CREATE TABLE statement
        column_definitions = ", ".join(f"{col} {data_type}" for col, data_type in schema.items())
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_definitions}
            )
        """

        # Execute the CREATE TABLE statement
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table {table_name} created successfully in '{db_file}'.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()


def create_table_with_list(db_file, table_name, columns):
    """
    Creates a SQLite table with specified column names.

    Args:
        db_file (str): The path to the SQLite database file.
        table_name (str): The name of the table to create.
        columns (list): A list of column names for the table.
    """

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Construct the CREATE TABLE statement
        column_definitions = ", ".join(f"{col} TEXT" for col in columns)
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_definitions}
            )
        """

        # Execute the CREATE TABLE statement
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table {table_name} created successfully in '{db_file}'.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()


def list_tables(db_file='operation.db'):
    """
    Lists all tables in a SQLite database.

    Args:
        db_file (str): The path to the SQLite database file.

    Returns:
        list: A list of table names in the database.
              Returns an empty list if no tables are found or if an error occurs.
    """

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # SQL query to retrieve table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

        # Fetch all results and extract table names
        tables = [row[0] for row in cursor.fetchall()]

        return tables

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return []  # Return an empty list in case of an error
    finally:
        if conn:
            conn.close()


def insert_row_from_dict(db_file, table_name, data_dict):
    """
    Inserts a row into a SQLite table using data from a dictionary.
    """

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Extract column names and values from the dictionary
        columns = ", ".join(data_dict.keys())
        placeholders = ", ".join("?" * len(data_dict))  # Create placeholders for parameterized query
        values = tuple(data_dict.values()) #Convert to tuple for parameterized query

        # Construct the INSERT statement
        sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
        """

        # Execute the INSERT statement using a parameterized query
        cursor.execute(sql, values)
        conn.commit()
        print("Row inserted successfully.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

 
def query_table(db_file, query, params=()):
    """
    Queries a SQLite table and returns the results as a list of tuples.
    """

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute(query, params)  # Execute the query with parameters

        results = cursor.fetchall()  # Fetch all the results

        return results

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return []  # Return an empty list in case of an error
    finally:
        if conn:
            conn.close()


def query_table_as_pandas(db_file, query, params=()):
    try:
        conn   = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute(query, params)  # Execute the query with parameters

        results         = cursor.fetchall()  # Fetch all the results
        df              = pd.DataFrame(results)
        column_names    = [description[0] for description in cursor.description]
        df.columns      = column_names
        return df

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return []  # Return an empty list in case of an error
    finally:
        if conn:
            conn.close()


def parse_svarga_format(text):
    lines = text.strip().split('\n')
    
    # Initialize the dictionary
    data = defaultdict(str)
    
    # Initialize section tracking
    current_section = None
    
    for line in lines:
        line = line.strip()
        
        # Skip format header and separator lines
        if line.startswith('FORMAT SVARGA') or line.startswith('====='):
            current_section = None
            continue
        
        # Check if line contains a colon for key-value pairs
        if ':' in line:
            parts      = line.split(':', 1)
            key        = parts[0].strip().strip('*')
            value      = parts[1].strip()
            ikey       = key.lower().strip() 
            ivalue     = value.lower().strip()            

            # Handle the top-level entries
            if current_section is None:
                data[ikey] = ivalue
            else:
                # Ensure the section exists in the dictionary
                if current_section not in data:
                    data[current_section] = defaultdict(str)
                
                data[current_section][ikey] = ivalue
        
        # Lines without colons could be section headers
        elif line and not line.startswith('*') and not ':' in line:
            current_section = line.lower().strip()
    
    return data


def parse_delivery_report(text):
    # Split the text by newlines and remove any empty lines
    lines = [line for line in text.strip().split('\n') if line]
    
    result = {}
    
    # Process the lines in pairs
    for i in range(0, len(lines), 2):
        if i + 1 < len(lines):
            column_name = lines[i].strip()
            value = lines[i + 1].strip()
            
            # Convert value to appropriate type if possible
            if value.lower() == 'null':
                value = None
            # elif value.replace('.', '', 1).isdigit():  # Check if it's a numeric value
            #     # Convert to float or int as appropriate
            #     if '.' in value:
            #         value = float(value)
            #     else:
            #         value = int(value)
                    
            result[column_name] = str(value)
            
    return result    


def check_delivery_id_existence(delivery_id):
    res = True
    query       = "select distinct delivery_id from delivery;"
    sf          = query_table(database_file, query)
    rf          = []
    for s in sf:
        rf.append(s[0])

    print(delivery_id, rf)
    if delivery_id in rf:
        print("delivery id in id_list")
        res = True
    else:
        print("delivery id NOT in id_list")
        res = False
    return res


def generate_delivery_rowrep(text):
    parsed         = parse_delivery_report(text)
    injection_date = str(parsed["delivery_date"]).lower()
    arrival_time   = str(parsed["delivery_arrival_time"])
    customer_id    = str(parsed["customer_id"])

    # Convert injection_date (DD/MM/YY) to datetime object
    injection_date_datetime = datetime.datetime.strptime(injection_date, '%d-%b-%y')

    # Convert arrival_time (HH.MM) to datetime object with injection_date
    arrival_time_time  = datetime.datetime.strptime(arrival_time, "%H:%M").time()  # Extract the time
    arrival_datetime   = datetime.datetime.combine(injection_date_datetime.date(), arrival_time_time) # Combine with the correct date
    arrival_time_iso   = arrival_datetime.strftime("%Y-%m-%d %H:%M:%S")
    injection_date_iso = injection_date_datetime.strftime("%Y-%m-%d") # Format injection date to iso    
    delivery_report_id = customer_id + re.sub(r"[\-\s:]", "", arrival_time_iso)[:-2]

    if check_delivery_id_existence(delivery_report_id):
        return None

    rowrep = {
        "delivery_id"               : delivery_report_id,
        "customer_id"               : customer_id,
        "delivery_route"            : parsed["delivery_route"],	
        "transport_plate_number"    : parsed["transport_plate_number"],	 
        "arrival_timestamp"         : arrival_time_iso,
        "pre_buffer_pressure"       : parsed["pre_buffer_pressure"], 	
        "delivery_stand_meter"      : parsed["delivery_stand_meter"],
        "delivery_pressure"         : parsed["delivery_pressure"],
        "delivery_temperature"      : parsed["delivery_temperature"], 	
        "post_buffer_pressure"      : parsed["post_buffer_pressure"], 	
        "transport_bank_pressure"   : parsed["transport_bank_pressure"],
    }

    return rowrep



def parse_restock_report(text):
    # Split the text by newlines and remove any empty lines
    lines = [line for line in text.strip().split('\n') if line]
    
    result = {}
    
    # Process the lines in pairs
    for i in range(0, len(lines), 2):
        if i + 1 < len(lines):
            column_name = lines[i].strip()
            value = lines[i + 1].strip()
            
            # Convert value to appropriate type if possible
            if value.lower() == 'null':
                value = None
            # elif value.replace('.', '', 1).isdigit():  # Check if it's a numeric value
            #     # Convert to float or int as appropriate
            #     if '.' in value:
            #         value = float(value)
            #     else:
            #         value = int(value)
                    
            result[column_name] = str(value)
            
    return result    



def generate_restock_rowrep(text):
    row             = parse_restock_report(text)
    plate_number    = row["transport_plate_number"]
    restock_date    = row["restock_date"]
    restock_id      = re.sub(r"[\s:]", "", plate_number) + re.sub(r"[\-\s:]", "", restock_date)
    restock_date    = datetime.datetime.strptime(restock_date, "%Y-%m-%d")
    restock_volume  = float(row["restock_volume"])

    rowrep = {
        "restock_id"              : restock_id,
        "transport_plate_number"  : plate_number,
        "restock_date"            : restock_date,
        "restock_volume"          : restock_volume,
        "gas_station_address"     : row["spbg_address"]
    }


    return rowrep





def extract_time_only(df, datetime_column):
    """
    Extracts only the time part from datetime strings in a column.
    
    Args:
        df: pandas DataFrame
        datetime_column: name of the column containing datetime strings
        
    Returns:
        pandas Series with only the time part
    """
    # Convert the strings to datetime objects first
    datetime_series = pd.to_datetime(df[datetime_column])
    
    # Extract only the time part
    time_only = datetime_series.dt.strftime('%H:%M:%S')
    
    return time_only


def get_formatted_delivery_df(df, colnames=None):
    # df['arrival_time']  = extract_time_only(df, 'arrival_timestamp')
    # df['finish_time']   = extract_time_only(df, 'finish_timestamp')
    
    # Create a Pandas Excel writer using XlsxWriter as the engine
    # df.rename(columns=colnames, inplace=True)
    return df


def export_delivery_to_excel(df, filename, colnames=None):
    df['arrival_time']  = extract_time_only(df, 'arrival_time')
    df['finish_time']   = extract_time_only(df, 'finish_time')
    
    
    # Create a Pandas Excel writer using XlsxWriter as the engine
    df.rename(columns=colnames, inplace=True)
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        # Convert the DataFrame to an Excel file
        df.to_excel(writer, sheet_name='Sheet1')
        
        # Get the xlsxwriter workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
    
        # Create a cell format with text wrapping enabled
        header_format = workbook.add_format({
            'text_wrap': True,  # Enable text wrapping
            'valign'   : 'vcenter',    
            'align'    : 'center',    
            'bold'     : True      
        })
        
        # Apply the multiline format to the header row (row 0)
        for col_num, value in enumerate(df.columns.values):
            # +1 to account for the index column
            worksheet.write(0, col_num + 1, value, header_format)
    
        # Set row height for the header row to accommodate multiple lines
        worksheet.set_row(0, 40)  # Height in points
        
        # Set the column width based on the maximum length in each column
        colnames  = list(df.columns)
        widthlist = []
        for col in colnames:
            max_len = max(df[col].astype(str).map(len).max(), len(col))
            widthlist.append(max_len + 2)
        
        for idx, col in enumerate(df.columns):
            colwidth = widthlist[idx]
            worksheet.set_column(idx + 1, idx + 1, colwidth)
        
        # Also adjust the index column (column 0)
        max_index_len = max(len(str(i)) for i in df.index)
        index_width   = max(max_index_len, len(str(df.index.name) if df.index.name else '')) + 2
        worksheet.set_column(0, 0, index_width)


def remove_table(db_file, table_name):
    """
    Removes a table from a SQLite database.

    Args:
        db_file (str): The path to the SQLite database file.
        table_name (str): The name of the table to remove.
    """

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Construct the DROP TABLE statement
        drop_table_sql = f"DROP TABLE IF EXISTS {table_name}"

        # Execute the DROP TABLE statement
        cursor.execute(drop_table_sql)
        conn.commit()
        print(f"Table {table_name} removed successfully from '{db_file}'.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()


def remove_all_tables(db_file='operation.db'):
    """
    Removes all tables from a SQLite database.

    Args:
        db_file (str): The path to the SQLite database file.
    """

    try:
        conn    = sqlite3.connect(db_file)
        cursor  = conn.cursor()

        tablenames = list_tables(db_file)
        # Construct the DROP TABLE statement
        for table_name in tablenames:
            drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
            cursor.execute(drop_table_sql)

        # Execute the DROP TABLE statement
        conn.commit()
        print(f"All tables removed successfully from '{db_file}'.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()


def json_to_dataframe(json_file_path):
    try:
        # Use pandas' built-in JSON reader. 
        # orient='records' is suitable for JSON structured as a list of dicts.
        df = pd.read_json(json_file_path, orient='records', dtype=str, convert_dates=False) 

        print(f"Successfully loaded data from {json_file_path}")
        return df

    except FileNotFoundError:
        print(f"Error: File not found at {json_file_path}")
        return None
    except ValueError as e: 
        # This can catch errors if the JSON is malformed or if pandas has trouble parsing
        print(f"Error: Could not parse JSON file {json_file_path}. Invalid JSON format or structure? Error: {e}")
        return None
    except Exception as e: 
        # Catch any other unexpected errors
        print(f"An unexpected error occurred: {e}")
        return None


def yaml_to_dataframe_as_string(yaml_file_path):
    try:
        # Read the YAML file
        with open(yaml_file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)

        # Check if the loaded data is a list of dictionaries
        if not isinstance(yaml_data, list):
            print(f"Error: YAML file {yaml_file_path} does not contain a list of dictionaries.")
            return None

        # Convert all values to strings within the YAML data
        stringified_data = []
        for item in yaml_data:
            stringified_item = {}
            if isinstance(item, dict):  # Check item is a dictionary
                for key, value in item.items():
                    sval = str(value)
                    stringified_item[key] = sval if value is not None else None
                stringified_data.append(stringified_item)
            else:
                print(f"Warning: Skipping non-dictionary item: {item}")

        # Convert to JSON format for pandas to read. CRITICAL step!
        json_string = json.dumps(stringified_data) # Convert data directly to JSON.
        df = pd.read_json(StringIO(json_string), orient='records', dtype=str, convert_dates=False)  # Use StringIO for pandas

        print(f"Successfully loaded data from {yaml_file_path} with all columns as strings.")
        return df

    except FileNotFoundError:
        print(f"Error: File not found at {yaml_file_path}")
        return None
    except yaml.YAMLError as e:
        print(f"Error: Could not parse YAML file {yaml_file_path}. Invalid YAML format? Error: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def get_applied_price(db_file, customer_id):
    """
    Retrieves the applied price for a customer based on their customer_id.
    """
    query = "SELECT applied_price FROM customer WHERE customer_id = ?"
    try:
        result = query_table(db_file, query, (customer_id,))
        if result:
            return float(result[0][0])  # Return the applied price as a float
        else:
            print(f"No applied price found for customer_id: {customer_id}")
            return None
    except Exception as e:
        print(f"An error occurred while retrieving applied price: {e}")
        return None
    

def get_minimum_monthly_volume(db_file, customer_id):
    """
    Retrieves the minimum monthly volume for a customer based on their customer_id.
    """
    query = "SELECT minimum_monthly_volume FROM customer WHERE customer_id = ?"
    try:
        result = query_table(db_file, query, (customer_id,))
        if result:
            return float(result[0][0])  # Return the minimum monthly volume as a float
        else:
            print(f"No minimum monthly volume found for customer_id: {customer_id}")
            return None
    except Exception as e:
        print(f"An error occurred while retrieving minimum monthly volume: {e}")
        return None
    

def get_customer_name(db_file, customer_id):
    """
    Retrieves the customer name based on their customer_id.
    """
    query = "SELECT customer_name FROM customer WHERE customer_id = ?"
    try:
        result = query_table(db_file, query, (customer_id,))
        if result:
            return result[0][0]  # Return the customer name
        else:
            print(f"No customer name found for customer_id: {customer_id}")
            return None
    except Exception as e:
        print(f"An error occurred while retrieving customer name: {e}")
        return None
    

def get_liter_weight_capacity(db_file, customer_id):
    """
    Retrieves the liter weight capacity for a customer based on their customer_id.
    """
    query = "SELECT liter_weight_capacity FROM customer WHERE customer_id = ?"
    try:
        result = query_table(db_file, query, (customer_id,))
        if result:
            return float(result[0][0])  # Return the liter weight capacity as a float
        else:
            print(f"No liter weight capacity found for customer_id: {customer_id}")
            return None
    except Exception as e:
        print(f"An error occurred while retrieving liter weight capacity: {e}")
        return None
    

def generate_charge_table(db_file, customer_id, start_date, end_date, vol_balance=0):
    """
    Generates a charge table for a specific customer and date range.
    """
    
    target_customer_id  = customer_id
    query               = "select * from delivery;"
    delivery            = pl.DataFrame(query_table_as_pandas(db_file, query))
    price               = get_applied_price(database_file, target_customer_id)

    # Filter and transform in a single operation
    df = (delivery
        .filter(pl.col("customer_id") == target_customer_id)
        .select([
            pl.col("arrival_timestamp"),
            pl.col("customer_id"),
            pl.col("delivery_stand_meter").map_elements(utils.safe_float, return_dtype=pl.Float64).alias("std_meter_on_arrival"),
            pl.col("delivery_pressure").map_elements(utils.safe_float, return_dtype=pl.Float64),
            pl.col("delivery_temperature").map_elements(utils.safe_float, return_dtype=pl.Float64)
        ])
        .with_columns([
            pl.col("arrival_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").dt.date().alias("date"),
            pl.col("arrival_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").dt.strftime("%A").alias('day')
        ])
        .drop("arrival_timestamp")
    )

    # Reorder columns
    df = df.select([
        'date',
        'day',
        'customer_id',
        'std_meter_on_arrival',
        'delivery_pressure',
        'delivery_temperature'
    ])

    # Compute differences and derived columns in a single operation
    df = df.with_columns([
        pl.col("std_meter_on_arrival").diff().alias("std_meter_diff"),
    ])

    # Create a reusable expression for the complex calculation
    corrected_volume_expr = (
        pl.col("std_meter_diff") * 
        (pl.col("delivery_pressure") + P_ATM) / P_ATM * 
        300 / (pl.col("delivery_temperature") + 273) * 
        (1 + CPF * pl.col("delivery_pressure"))
    )

    # Add all computed columns in a single operation
    df = df.with_columns([
        corrected_volume_expr.alias("charged_volume"),
        (corrected_volume_expr * price).alias("charged_price"),
        (
            pl.col("customer_id").cast(pl.Utf8) +
            pl.col("date").cast(pl.Utf8).str.replace("-", "", literal=True, n=2) + 'C'
        ).alias("charge_id")
    ])

    # Reorder columns
    df = df.select([
        'customer_id',
        'date',
        'day',
        'charged_price',
        'charged_volume',
        'std_meter_on_arrival',
        'delivery_pressure',
        'delivery_temperature',
        'std_meter_diff',
    ])

    # Convert the list of dates to a Polars expression for efficient filtering.
    pl_start_date       = strptime(start_date, "%Y-%m-%d").date()
    pl_end_date         = strptime(end_date, "%Y-%m-%d").date()
    filter_expression   = pl.col("date").is_between(pl_start_date, pl_end_date)
    sel_df              = df.filter(filter_expression)

    pretotal_volume    = sel_df["charged_volume"].sum()
    pretotal_price     = sel_df["charged_price"].sum()
    price_balance      = vol_balance * price

    total_volume = pretotal_volume + vol_balance
    total_price  = pretotal_price + price_balance

    tax_coef                   =  0.11
    charged_tax                = total_price * tax_coef
    total_price_wtax           = total_price + charged_tax


    # convert english day name to indonesian
    sel_df = sel_df.with_columns([
        pl.col("day").replace(day_name_map).alias("day")
    ])

    res = {
        "dataframe"             : sel_df,
        "unit_price"            : price,
        "pretotal_volume"       : pretotal_volume,
        "pretotal_price"        : pretotal_price,
        "volume_balance"        : vol_balance,
        "price_balance"         : price_balance,
        "total_volume"          : total_volume,
        "total_price"           : total_price,
        "tax_coef"              : tax_coef,
        "charged_tax"           : charged_tax,
        "total_price_wtax"      : total_price_wtax,
    }

    return res


def get_transports_as_options():
    db_file = 'operation.db'    
    query   = "select distinct transport_plate_number FROM delivery;"
    
    try:
        result = query_table(db_file, query)
    except Exception as e:
        print(f"An error occurred while retrieving transports list {e}")
    
    options = []
    for i, res in enumerate(result):
        val  = res[0]
        rdict = {
            "label": val,
            "value": val
        }
        options.append(rdict)
    return options
    

def generate_tracker_set(target_plate_number):
    db_file             = 'operation.db'
    query               = "select * from delivery;"
    delivery            = pl.DataFrame(query_table_as_pandas('operation.db', query))

    # select by target plate number
    filter_expression   = pl.col("transport_plate_number") == target_plate_number

    # select relevant column
    deliv               = (delivery
        .filter(filter_expression)
        .select([
            pl.col("transport_plate_number"),
            pl.col("customer_id"),
            pl.col("arrival_timestamp"),
            pl.col("pre_buffer_pressure").map_elements(utils.safe_float, return_dtype=pl.Float64),
            pl.col("post_buffer_pressure").map_elements(utils.safe_float, return_dtype=pl.Float64),
            pl.col("delivery_stand_meter").map_elements(utils.safe_float, return_dtype=pl.Float64).alias("std_meter_on_arrival"),
            pl.col("delivery_pressure").map_elements(utils.safe_float, return_dtype=pl.Float64),
            pl.col("delivery_temperature").map_elements(utils.safe_float, return_dtype=pl.Float64),
            pl.col("customer_id").map_elements(lambda x: get_liter_weight_capacity(db_file, x), return_dtype=pl.Float64).alias("liter_weight_capacity")
        ])
        .with_columns([
            pl.col("arrival_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").dt.date().alias("date"),
        ])
        .drop("arrival_timestamp")
        .with_columns([
            ((pl.col("post_buffer_pressure") - pl.col("pre_buffer_pressure"))/200.0 * pl.col("liter_weight_capacity")/4).alias("est_volume_out"),
            ((pl.col("post_buffer_pressure").shift(1).fill_null(0) - pl.col("pre_buffer_pressure"))/200.0 * pl.col("liter_weight_capacity").shift(1).fill_null(0)/4.).alias("est_volume_consumed"),
        ])
    )

    # Compute differences and derived columns in a single operation
    deliv = deliv.with_columns([
        pl.col("std_meter_on_arrival").diff().alias("std_meter_diff"),
    ])

    # Create a reusable expression for the complex calculation
    corrected_volume_expr = (
        pl.col("std_meter_diff") * 
        (pl.col("delivery_pressure") + P_ATM) / P_ATM * 
        300 / (pl.col("delivery_temperature") + 273) * 
        (1 + CPF * pl.col("delivery_pressure"))
    )

    # Add all computed columns in a single operation
    deliv = deliv.with_columns([
        corrected_volume_expr.alias("charged_volume")
    ])

    deliv   = deliv.select([
        "transport_plate_number",
        "date",
        "est_volume_out",
        "est_volume_consumed",
        "charged_volume",
    ])

    query               = "select * from restock;"
    restock             = pl.DataFrame(query_table_as_pandas('operation.db', query))
    filter_expression   = pl.col("transport_plate_number") == target_plate_number

    # select relevant column
    restock               = (restock
        .filter(filter_expression)
        .select([
            pl.col("transport_plate_number"),
            pl.col("restock_date"),
            pl.col("restock_volume").map_elements(utils.safe_float, return_dtype=pl.Float64),
            pl.col("gas_station_address")
        ])
        .with_columns([
            pl.col("restock_date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").dt.date().alias("date"),
        ])
        .drop("restock_date")
    )

    restock  = restock.select([
        "transport_plate_number",
        "date",
        "restock_volume",
        "gas_station_address"
    ])


    rf = restock[1:].with_columns([
        pl.col("restock_volume").cum_sum().alias("restock_volume_cumul")
    ]).select([
        "date",
        "restock_volume_cumul"
    ])


    df = deliv.with_columns([
        pl.col("est_volume_out").cum_sum().alias("volume_out_cumul"),
        pl.col("est_volume_consumed").cum_sum().alias("volume_consumed_cumul"),
        pl.col("charged_volume").cum_sum().alias("charged_volume_cumul")
    ]).select([
        "date",
        "volume_out_cumul",
        "volume_consumed_cumul",
        "charged_volume_cumul"
    ])


    # Create figure
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=rf['date'], 
            y=rf['restock_volume_cumul'], 
            # mode='lines+markers',
            name        = 'restock volume',
            marker_color= '#E91E63',
            opacity=0.7,
            width=86400000 * 0.25  # Width in milliseconds (2 days wide)

        )
    )

    # Add traces for each column
    fig.add_trace(
        go.Scatter(
            x=df['date'], 
            y=df['volume_out_cumul'], 
            mode='lines+markers',
            name='delivered volume est.',
            line=dict(color='#1E88E5', width=2)
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df['date'], 
            y=df['volume_consumed_cumul'], 
            mode='lines+markers',
            name='consumed volume est.',
            line=dict(color='#4CAF50', width=2)
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df['date'], 
            y=df['charged_volume_cumul'], 
            mode='lines+markers',
            name='charged volume',
            line=dict(color='#FFC107', width=2)
        )
    )

    # Update layout
    fig.update_layout(
        title           = dict(
            text        = 'Cumulative volume metrics over time',
            x           = 0.3,
        ),
        legend_title    = 'Metrics',
        template        = 'plotly_white',
        hovermode       = 'x unified',
        # width           = 1000,
        height          = 600,
        plot_bgcolor    = '#222222',
        paper_bgcolor   = '#222222',
        font            = dict(color='#FFFFFF'),
        xaxis           = dict(
            title       = "Date",
            showgrid    = False,
            # gridcolor   = "#333333",
            # gridwidth   = 0.5,
            # zeroline    = False
        ),
        yaxis=dict(
            title       = "Volume (m3)",
            showgrid    = True,
            gridcolor   = "#333333",
            gridwidth   = 0.5,
            zeroline    = False
        ),
    )

    # Add grid
    # fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    # fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')

    return fig