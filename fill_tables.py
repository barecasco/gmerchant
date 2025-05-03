import database as db
from importlib import reload
import pandas as pd
import datetime
import re
import utils

database_file   = 'operation.db'

def replenish_table():
    print("replenishing table")
    # -----------------------------------------------------------------------------------------
    # create tables
    # -----------------------------------------------------------------------------------------
    # do database reset here
    db.remove_all_tables()

    for table_name in db.db_table_columns:
        db.create_table_with_list(database_file, table_name, db.db_table_columns[table_name])


    # -----------------------------------------------------------------------------------------
    # customer table
    # -----------------------------------------------------------------------------------------
    table_name      = 'customer'
    db.remove_table(database_file, table_name)
    db.create_table_with_list(database_file, table_name, db.db_table_columns[table_name])
    df = db.yaml_to_dataframe_as_string('customer.yml')

    for i, row in df.iterrows():

        subscription_start     = row["subscription_start"]
        subs_start_iso         = datetime.datetime.strptime(subscription_start, "%Y-%m-%d")

        insertion = {
            "customer_id"             : row["customer_id"],          
            "customer_name"           : row["customer_name"],
            "customer_address"        : row["customer_address"],
            "subscription_type"       : row["subscription_type"],
            "subscription_start"      : subs_start_iso,
            "liter_weight_capacity"   : row["liter_weight_capacity"],
            "minimum_monthly_volume"  : row["minimum_monthly_volume"],
            "buffer_count"            : row["buffer_count"],
            "applied_price"           : row["applied_price"]
        }

        db.insert_row_from_dict(database_file, "customer", insertion)


    # check
    # query   = "select * from customer;"
    # df      = db.query_table_as_pandas('operation.db', query)

    # utils.display_string_in_notepad(df.to_string())


    # -----------------------------------------------------------------------------------------
    # delivery table    
    # -----------------------------------------------------------------------------------------
    table_name      = 'delivery' 
    db.remove_table(database_file, table_name)
    db.create_table_with_list(database_file, table_name, db.db_table_columns[table_name])

    df              = db.yaml_to_dataframe_as_string('delivery.yml')

    for i, row in df.iterrows():
        customer_id           = row["customer_id"]
        dlv_date              = row["delivery_date"]
        arrival_time          = row["delivery_arrival_time"]    
        dlv_date              = datetime.datetime.strptime(dlv_date, "%d-%b-%y")
        # Convert arrival_time to iso
        arrival_time          = datetime.datetime.strptime(arrival_time, "%H.%M").time()  # Extract the time
        arrival_datetime      = datetime.datetime.combine(dlv_date.date(), arrival_time) # Combine with the correct date
        arrival_time_iso      = arrival_datetime.strftime("%Y-%m-%d %H:%M:%S")    
        delivery_id           = customer_id + re.sub(r"[\-\s:]", "", arrival_time_iso)[:-2]
        arrival_timestamp     = arrival_time_iso

        insertion = {
            "delivery_id"            : delivery_id,
            "customer_id"            : row["customer_id"],
            "delivery_route"         : row["delivery_route"].lower(),
            "transport_plate_number" : row["plate_number"],
            "arrival_timestamp"      : arrival_timestamp,
            "pre_buffer_pressure"    : row["pre_buffer_pressure"],
            "delivery_stand_meter"   : row["delivery_stand_meter"],
            "delivery_pressure"      : row["delivery_pressure"],
            "delivery_temperature"   : row["delivery_temperature"],
            "post_buffer_pressure"   : row["post_buffer_pressure"],
            "transport_bank_pressure": row["transport_bank_pressure"],
        }

        db.insert_row_from_dict(database_file, "delivery", insertion)


    # check
    # query   = "select * from delivery;"
    # df      = db.query_table_as_pandas('operation.db', query)
    # utils.display_string_in_notepad(df.to_string())


    # -----------------------------------------------------------------------------------------
    # restock table    
    # -----------------------------------------------------------------------------------------
    table_name      = 'restock' 
    db.remove_table(database_file, table_name)
    db.create_table_with_list(database_file, table_name, db.db_table_columns[table_name])
    df = db.yaml_to_dataframe_as_string('restock.yml')

    for i, row in df.iterrows():
        plate_number    = row["plate_number"]
        restock_date    = row["restock_date"]
        restock_id      = re.sub(r"[\s:]", "", plate_number) + re.sub(r"[\-\s:]", "", restock_date)
        restock_date    = datetime.datetime.strptime(restock_date, "%Y-%m-%d")
        restock_volume  = float(row["restock_volume"])

        insertion = {
            "restock_id"        : restock_id,
            "transport_plate_number"      : plate_number,
            "restock_date"      : restock_date,
            "restock_volume"    : restock_volume,
            "gas_station_address"      : row["spbg_address"]
        }

        db.insert_row_from_dict(database_file, table_name, insertion)


    # check
    # query   = "select * from restock;"
    # df      = db.query_table_as_pandas('operation.db', query)

    # utils.display_string_in_notepad(df.to_string())


if __name__ == '__main__':
    replenish_table()