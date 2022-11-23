# connect to db through sql connection
import pandas as pd
import logging
import pymysql
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import json

# opens ssh tunnel to connect
def open_ssh_tunnel(verbose = False):
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
    global tunnel
    
    with open("credentials.json", "r") as credentials:
        credentials = json.load(credentials)
        
    tunnel = SSHTunnelForwarder(
        (credentials["ssh_host"], 22),
        ssh_username = credentials["ssh_username"],
        ssh_password = credentials["ssh_password"],
        remote_bind_address = ("127.0.0.1", 3306)
    )
    
    tunnel.start()

def mysql_connect(database):
    """Connect to a MySQL server using the SSH tunnel connection
    
    :return connection: Global MySQL database connection
    """
    global connection
    
    with open("credentials.json", "r") as credentials:
        credentials = json.load(credentials)
    
    connection = pymysql.connect(
        host = credentials["localhost"],
        user = credentials["database_username"],
        passwd = credentials["database_password"],
        db = database,
        port = tunnel.local_bind_port
    )
    
    
def run_query(sql):   
    return pd.read_sql_query(sql, connection)

def mysql_disconnect():
    connection.close()
    
def close_ssh_tunnel():
    tunnel.close
    
    
def connect_helper(database, query):
    open_ssh_tunnel()
    con = mysql_connect(database)
    df = run_query(query)
    mysql_disconnect()
    close_ssh_tunnel()
    return df

# connect to db and query relevant oi_datazoo tables in pandas format
codeset_column = connect_helper("oi_datazoo", "SELECT id, column_id, codeset_id FROM codeset_column")
columns = connect_helper("oi_datazoo", "SELECT id, datatable_id FROM columns")
codes = connect_helper("oi_datazoo", "SELECT codeset_id, code, definition FROM codes")


# gets all of the current codes listed in the dataset
def get_dat_col(num):
    column_id = codeset_column['column_id'][num-1]  # need to subtract 1 because of SQL indexing vs pandas
    codeset_id = codeset_column.loc[codeset_column['column_id'] == column_id]['codeset_id'].iloc[0]
    datatable_id = columns[columns['id'] == column_id]['datatable_id'].iloc[0]
    query = "SELECT * FROM dat_" + str(datatable_id)
    col = "col_" + str(column_id)
    db = connect_helper("oi_datazoo_datatables", query)[col]
    return db


# gets all of the codes that are supposed to be associated with the dataset
def get_codes(num):
    column_id = codeset_column['column_id'][num-1]  # need to subtract 1 because of SQL indexing vs pandas
    codeset_id = codeset_column.loc[codeset_column['column_id'] == column_id]['codeset_id'].iloc[0]
    code_list = codes.loc[codes['codeset_id'] == codeset_id]['code']
    return code_list


# grabs set of current codes and associated codes and compares them against each other
def get_sets(num):
    a = set(get_dat_col(num))
    b = set(get_codes(num))
    return a - b


# these are the codeset_columns primary ids that are involved with a dataset that has a codeset_column. Can be edited to add more in as needed.
codeset_column_ids = [94, 112, 122] 

need_update = {}
for item in codeset_column_ids:
    need_update[item] = get_sets(item)

print('The following codeset_columns need to be updated:')
print(invalid need_update)

