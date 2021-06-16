"""
Demo How to Use JSONparse
=========================

* **Program file**: test_jsonparse.py
* **Client**      : demo using sample JSON data file
* **Updates**     :

The purpose of this file is to demostrate importing data files to database.

Run this test under upper folder of `tests`

`python -B -m unittest tests.test_jsonparse.test_map_gen`

 or

`python -B -m unittest tests.test_jsonparse.test_parse`

 etc.

* The dbinterface.postgresql is used to connect to database
* You need to install it in your virtual environment

Common tests
------------
* **test_map_gen** will do the following:

  * load into **json_data**
  * Compute all paths in data
  * Generate map (table_plan_json)
  * export to JSON format map file
  * generate postgres DDL SQL query to file

* **test_map2csv** will do the following:

  * Import JSON format map file
  * Export to CSV format map file
  * Then user can modify CSV format map file as needed

* **test_csv2map** will do the following:

  * Import CSV format map file
  * generate postgres DDL SQL query to file (based on user changedd map)
  * Export to JSON format map file
  * Later user can parse data using this new map and import to database

* **test_parse** will do the following:

  * Decrypt data file
  * load into **json_data**
  * Import JSON format map file
  * Parse JSON data based on map
  * Import parsed data into database (assume tables in database has been created using DDL)

The python functions
--------------------
"""
import os
import unittest
import csv
from datetime import datetime
import traceback # Python error trace
import logzero
from logzero import logger

try:
    from jsonparse.jsonutils import JsonUtils
except:
    import sys
    sys.path.insert(0, os.path.abspath('jsonparse'))
    print(sys.path)
    from jsonutils import JsonUtils

from dbinterface.sql import Sql

def GetPostgreSQLLoginInfo():
    """
    * Get database login information from pem file
    """
    passfile = '/mnt/data/other/pem/sinnud_pg.dat'
    with open(passfile, 'r') as f:
        passinfo = f.read().strip()
    (host, user, dbname, password, port) = passinfo.split()
    if os.path.isfile(passfile):
        return (True, (host, user, dbname, password, port))
    return (False, None)

def get_db_conn():
    """ define database connection using Sql under dbinterface
    """
    (getpass, (host, user, dbname, password, port)) = GetPostgreSQLLoginInfo()
    if not getpass:
        print(f"Failed to get password information from pem file!!!")
        exit(1)
    return Sql(host, dbname, user=user, passwd=password, port=port)

def flush_to_db(ju=None, db_conn=None, schema=None, truncate_before_flush=True):
    """ flush data from memory to greenplum tables
    """
    for tbl_map in ju.map["tableList"]:
        tbl = tbl_map["tableName"]
        if truncate_before_flush:
            qry = f"truncate table {schema}.{tbl}"
            db_conn.sql_execute_with_replace(qry)
        qry = f"copy {schema}.{tbl} FROM STDIN with DELIMITER '|'"
        from io import StringIO
        db_conn.import_from_file(qry, StringIO('\n'.join(ju.parsed_tables[tbl])))
    pass

class TestJSONparse(unittest.TestCase):
    def test_map_gen(self):
        """ test function: generate map based on data
        """
        crt_dir = os.path.dirname(__file__)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        script_fn = os.path.basename(__file__).replace('.py', '')
        # mylog = f"{crt_dir}/{script_fn}_{timestamp}.log"
        mylog = f"{crt_dir}/{script_fn}.log"
        if os.path.exists(mylog):
            os.remove(mylog)
        logzero.logfile(mylog)

        logger.info(f'start python code {__file__}.\n')
        data_map_dir = f"{crt_dir}/../../data"
        json_df = f"{data_map_dir}/txn8698.json"
        with open(json_df, 'r') as f:
            json_data = f.read()
        logger.debug(f"raw data string length: {len(json_data)}")
        
        ju = JsonUtils(csv_delim='|', table_name_prefix='ex_')
        ju.load_from_string(jstr = json_data)
        
        ju.compute_all_paths()
        ju.table_plan_json()
        ju.json_map_export(map_file = f"{data_map_dir}/ex_init.map")
        ju.postgres_ddl(sql_file = f"{data_map_dir}/ex_init.sql", schema_name = 'cvs')
        
        logger.info(f'end python code {__file__}.\n')


    def test_parse(self):
        """ test function: parse JSON data based on map; import into database
        """
        crt_dir = os.path.dirname(__file__)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        script_fn = os.path.basename(__file__).replace('.py', '')
        # mylog = f"{crt_dir}/{script_fn}_{timestamp}.log"
        mylog = f"{crt_dir}/{script_fn}.log"
        if os.path.exists(mylog):
            os.remove(mylog)
        logzero.logfile(mylog)

        logger.info(f'start python code {__file__}.\n')
        data_map_dir = f"{crt_dir}/../../data"
        json_df = f"{data_map_dir}/txn8698.json"
        with open(json_df, 'r') as f:
            json_data = f.read()
        logger.debug(f"raw data string length: {len(json_data)}")
        
        ju = JsonUtils(csv_delim='|', table_name_prefix='ex_')
        ju.load_from_string(jstr = json_data)

        ju.json_map_import(map_file = f"{data_map_dir}/ex_init.map")
        ju.parse_to_csv()
        # flush to db?
        flush_to_db(ju=ju, db_conn=get_db_conn(), schema='cvs')

        logger.info(f'end python code {__file__}.\n')

    def test_map2csv(self):
        """ test function: import map to CSV file for user revising
        """
        crt_dir = os.path.dirname(__file__)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        script_fn = os.path.basename(__file__).replace('.py', '')
        # mylog = f"{crt_dir}/{script_fn}_{timestamp}.log"
        mylog = f"{crt_dir}/{script_fn}.log"
        if os.path.exists(mylog):
            os.remove(mylog)
        logzero.logfile(mylog)

        logger.info(f'start python code {__file__}.\n')
        ju = JsonUtils(csv_delim='|', table_name_prefix='ex_')
        data_map_dir = f"{crt_dir}/../../data"

        ju.json_map_import(map_file = f"{data_map_dir}/ex_init.map")
        ju.map_export_csv(map_csv = f"{data_map_dir}/ex_init.csv")
        logger.info(f'end python code {__file__}.\n')


    def test_csv2map(self):
        """ test function: import CSV format map
        """
        crt_dir = os.path.dirname(__file__)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        script_fn = os.path.basename(__file__).replace('.py', '')
        # mylog = f"{crt_dir}/{script_fn}_{timestamp}.log"
        mylog = f"{crt_dir}/{script_fn}.log"
        if os.path.exists(mylog):
            os.remove(mylog)
        logzero.logfile(mylog)

        logger.info(f'start python code {__file__}.\n')
        ju = JsonUtils(csv_delim='|', table_name_prefix='ex_')
        data_map_dir = f"{crt_dir}/../../data"

        ju.map_import_csv(map_csv = f"{data_map_dir}/ex_v001.csv")
        ju.postgres_ddl(sql_file = f"{data_map_dir}/ex_v001.sql", schema_name = 'cvs')
        ju.json_map_export(map_file = f"{data_map_dir}/ex_v001.map")
        logger.info(f'end python code {__file__}.\n')

if __name__ == '__main__':
    unittest.main()