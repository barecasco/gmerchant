import database as db
database_file   = 'operation.db'

# -----------------------------------------------------------------------------------------
# create tables
# -----------------------------------------------------------------------------------------
# do database reset here
db.remove_all_tables()
for table_name in db.db_table_columns:
    db.create_table_with_list(database_file, table_name, db.db_table_columns[table_name])
