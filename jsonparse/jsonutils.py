"""
The JSON solution
=================

- **File name**: json_utils.py
- **Arthor**: Luke Du
- **Updates**:

  - 17JUN2021-LD load multiple JSON data. Instead of just load one data, change code to append way

Parse JSON data directly using JSON format mapping.

JsonUtils CLASS
---------------
"""
import os
from datetime import datetime
import traceback # Python error trace
import logzero
from logzero import logger
import json

import uuid # random string generator

import csv

# get full path
# from https://stackoverflow.com/questions/51488240/python-get-json-keys-as-full-path
import collections

class JsonUtils(object):
    """ 
    **variable member initialization in __init__ function**

    - **csv_delim**:

      * specific symbol for csv format
      * The symbol in json data need to removed/replaced before parsing
      * Used for special symbol when import data into database
    
    - **json_txn_id_name**:

      * specific column name for json data
      * One json record may be parsed to several tables
      * This column identify which record it is
      * It will be used to link tables together
    
    - **table_name_prefix**: Optional, specify table name with this prefix
    - **flag_json_array**:
    
      * Special string                                            
      * When go through json data, indicate the tag is json list  
      * Later this tag will becomes separate table                
    
    - **json_data**: data loaded in json format using json module
    - **pathlist**: all path in data, columns in table later
    - **arraylist**: all array in data, tables later
    - **map**:
    
      * JSON format map                                                       
      * can be generated by self.table_plan_json                              
      * can be exported to JSON format by self.json_map_export                
      * can be loaded from JSON file by self.json_map_import                  
      * can be exported to CSV format for customization by self.map_export_csv
      * can be loaded from CSV file with customization by self.map_import_csv 
    
        
    - **parsed_tables**: parsed cvs tables
    - **map_path**: path list from map
    - **map_array**: array list from map
    """

    def __init__(self, 
                 csv_delim = ',', 
                 json_txn_id_name = 'txn_uuid',
                 table_name_prefix = '',
                 flag_json_array = '__JSON_array__',
                ):
        self.csv_delim = csv_delim
        self.json_txn_id_name = json_txn_id_name
        self.table_name_prefix = table_name_prefix
        self.flag_json_array = flag_json_array
        self.json_data = None
        self.pathlist = None
        self.arraylist = None
        self.map = None
        self.map_path = None

    def load_from_file(self, df=None):
        """ 
        *load JSON data from file*

        * Valid JSON data can be JSON list(*[]*). The JSON lines (one line per JSON transaction) may not work.
        * JSON data stored into **json_data**
        """
        try:
            with open(df, 'r') as f:
                self.json_data = json.load(f)
        except:
            print(f"{traceback.format_exc()}")
            exit(1)

    def load_from_string(self, jstr=None):
        """ 
        *load JSON data from string*
        
        * Valid JSON data can be JSON list(*[]*). The JSON lines (one line per JSON transaction) may not work.
        * JSON data stored into **json_data**
        """
        try:
            self.json_data = json.loads(jstr)
        except:
            print(f"{traceback.format_exc()}")
            exit(1)

    def load_from_list(self, jsonlist=None):
        """ 
        *load JSON data from data list*

        * Convert *jsonlist* to *jstr* then call **load_from_string**
        """
        mystr = ','.join(jsonlist)
        jstr = f"[{mystr}]"
        self.load_from_string(jstr = jstr)

    def append_from_list(self, jsonlist=None):
        """ 
        *Append to JSON data from JSON list*

        * use list extend method
        """
        self.json_data.extend(jsonlist)

    def get_json_len(self):
        """ 
        * output length of JSON data
        """
        return len(self.json_data)

    def compute_all_paths(self):
        """ 
        *Compute all paths in JSON data*

        * call **get_paths** out of this class (see below), work on **json_data**
        * Store output into **arraylist** (table) and **pathlist**
        """
        try:
            allpathlist = get_paths(self.json_data, self.flag_json_array)
            # can multithread do so
        except:
            print(f"{traceback.format_exc()}")
            exit(1)
        pathset = set()
        arrset = set()
        for path in allpathlist:
            if self.flag_json_array in path:
                path.remove(self.flag_json_array)
                if len(path) > 0:
                    arrset.add('.'.join(path))
            else:
                pathset.add('.'.join(path))
        pathlist = list(sorted(pathset))
        self.arraylist = list(sorted(arrset))
        #self.pathlist = [x for x in pathlist if x not in self.arraylist]
        valid_list = []
        for x in reversed(pathlist):
            if x not in self.arraylist and len(valid_list) == 0:
                valid_list.append(x)
            elif x not in self.arraylist and x not in valid_list[-1]:
                valid_list.append(x) # remove object
        self.pathlist = sorted(valid_list)

    def table_plan_json(self):
        """ 
        *create map in json format*

        * Based on **arraylist** and **pathlist**, compute map in Python dictionary
        * Store map (python dictionary) into **map**
        """
        table_name_list = []
        total_path = self.pathlist.copy()
        j_tbllist = list()
        for idx, path in reversed(list(enumerate(self.arraylist, start=1))):
            # use the last field in path as table name
            # if exist, random generate one for user to rename in future
            j_tbl = dict()
            tbl = name_from_path(path, table_name_list)
            table_name = f"{self.table_name_prefix}{idx:02d}{tbl}"
            table_name_list.append(table_name)
            j_tbl["tableName"] = table_name
            j_tbl["rootPath"] = path
            # j_tbl["seqList"] = [{"columnName":f"seq_{p}"} for p in path.split('.')]
            j_tbl["seqList"] = table_seq_list(path, self.arraylist)
            logger.info(f"Collect paths for {idx}-th table {table_name}...")
            table_path = [p for p in total_path if path in p]
            j_clmlist = list()
            table_column = []
            for p in table_path:
                j_clm = dict()
                clm = name_from_path(p, table_column)
                table_column.append(clm)
                j_clm["columnName"] = clm
                j_clm["relativePath"] = p[len(path)+1:]
                j_clmlist.append(j_clm)
            for p in table_path:
                total_path.remove(p)
            j_tbl["columnList"] = j_clmlist
            j_tbllist.append(j_tbl)
        j_tbl = dict()
        table_name = f"{self.table_name_prefix}00root"
        j_tbl["tableName"] = table_name
        j_tbl["rootPath"] = ''
        logger.info(f"Collect paths for root table {table_name}...")
        table_column = []
        j_clmlist = list()
        for p in total_path:
            j_clm = dict()
            clm = name_from_path(p, table_column)
            table_column.append(clm)
            j_clm["columnName"] = clm
            j_clm["relativePath"] = p
            j_clmlist.append(j_clm)
        j_tbl["columnList"] = j_clmlist
        j_tbllist.append(j_tbl)

        j_map = dict()
        j_map["tableNumber"] = len(self.arraylist) + 1
        j_map["tableList"] = j_tbllist

        # mystr = json.dumps(j_map, indent=4)
        # logger.info(f"DEBUG:\n{mystr}")

        self.map = j_map

    def json_map_export(self, map_file=None):
        """
        *export map as JSON format*

        * export map into *map_file*
        * later the JSON map file can be used to load JSON data
        * user can modify (not recommanded. We recommand to load JSON map; export to csv file; edit csv file; load from csv file, and export modified map in JSON format)
        """
        mystr = json.dumps(self.map, indent=4)
        with open(map_file, 'w') as f:
            f.write(mystr)

    def json_map_import(self, map_file=None):
        """
        *import map from JSON format*
        """
        try:
            with open(map_file, 'r') as f:
                self.map = json.load(f)
        except:
            print(f"{traceback.format_exc()}")
            exit(1)
        # mystr = json.dumps(self.map, indent=4)
        # logger.info(f"DEBUG:\n{mystr}")

    def map_export_csv(self,map_csv=None):
        """
        *export map as csv format*

        * user can modify and import again
        """
        with open(map_csv,'w') as f:
            tbl_cnt = self.map["tableNumber"]
            f.write(f"{tbl_cnt},table name,root path,,\n")
            for table_elm in self.map["tableList"]:
                tbl_name = table_elm["tableName"]
                tbl_rpth = table_elm["rootPath"]
                f.write(f",{tbl_name},{tbl_rpth},,\n")

            for table_elm in self.map["tableList"]:
                tbl_name = table_elm["tableName"]
                tbl_rpth = table_elm["rootPath"]
                seq_list = table_elm.get("seqList", None)
                if seq_list is not None:
                    f.write(f",,,,\n,,,,\n{tbl_name},{len(seq_list)},column name,array path,\n")
                    for seq in seq_list:
                        clm_name = seq.get("columnName", None)
                        arr_p = seq.get("arrayPath", None)
                        f.write(f",,{clm_name},{arr_p},\n")
                    f.write(",,,\n")
                else:
                    f.write(",,,,\n,,,,\n")

                clm_lst = table_elm["columnList"]
                f.write(f"{tbl_name},{len(clm_lst)},relative path,path base, full path\n")
                for clm in clm_lst:
                    clm_name = clm["columnName"]
                    rel_path = clm["relativePath"]
                    if len(tbl_rpth) < 1:
                        full_path = rel_path
                    else:
                        full_path = f"{tbl_rpth}.{rel_path}"
                    # f.write(f"{tbl_name},{clm_name},{rel_path},{full_path}\n")
                    path_base = rel_path.split('.')[-1]
                    f.write(f",{clm_name},{rel_path},{path_base},{full_path}\n")

    def map_import_csv(self,map_csv=None):
        """
        *import map from csv format*

        * the tool to change JSON map
        """
        # reading csv file
        rows = []
        with open(map_csv, 'r') as csvfile: 
            # creating a csv reader object (can NOT let rows = csv.reader(csvfile))
            csvreader = csv.reader(csvfile) 
            for row in csvreader:
                rows.append(row)

        # pnt is the point to each row of csv file
        pnt = 0
        # store the map info into imp_map (imported map)
        imp_map = dict()
        tbl_cnt = int(rows[pnt][0]) # the first line of csv, only use table count
        imp_map["tableNumber"] = tbl_cnt
        pnt += 1 # table list header line
        tbl_list = [row[1:3] for row in rows[pnt:pnt+tbl_cnt]] # table name and array path
        pnt += int(tbl_cnt) # table list line
        # tableList array, will assign to self.map["tableList"]
        map_tbl_lst = []
        for idx, tbl in enumerate(tbl_list, start=1):
            map_tbl = dict()
            map_tbl["tableName"] = tbl[0]
            map_tbl["rootPath"] = tbl[1]
            pnt += 2 # two empty line before each table information
            if len(rows[pnt][4]) == 0: # array, need sequnce number variables
                seq_var_cnt = int(rows[pnt][1])
                pnt += 1 # table sequence variable information
                seq_lst = [{"columnName":e[2], "arrayPath": e[3]} for e in rows[pnt:pnt+seq_var_cnt]]
                pnt += seq_var_cnt # table sequence lines
                map_tbl["seqList"] = seq_lst
                pnt += 1 # empty line between sequence block and column block
            clm_cnt = int(rows[pnt][1])
            pnt += 1 # table column information
            clm_lst = [{"columnName": e[1], "relativePath": e[2]} for e in rows[pnt:pnt+clm_cnt]]
            pnt += clm_cnt # table column lines
            map_tbl["columnList"] = clm_lst
            # logger.debug(map_tbl)
            map_tbl_lst.append(map_tbl)
        imp_map["tableList"] = map_tbl_lst
        self.map = imp_map
        # logger.debug(f"Now: {self.map}")

    def postgres_ddl(self, sql_file = None, schema_name = "default_schema"):
        """ 
        *generate postgresql queries of table DDL based on map*

        * Based on **map**, create table DDL using *schema_name*
        * Store SQL query into *sql_file*
        """
        tbl_lst = self.map["tableList"]
        
        str_buff = ""
        for idx, tbl in enumerate(tbl_lst, start=1):
            tbl_name = tbl["tableName"]
            mystr = f"drop table if exists {schema_name}.{tbl_name};"
            mystr = f"{mystr}\ncreate table {schema_name}.{tbl_name} ("
            mystr = f"{mystr}\n    uuid text"
            seq_lst = tbl.get("seqList", None)
            if seq_lst is not None:
                seq_str = ' int\n    , '.join([e["columnName"] for e in seq_lst])
                mystr = f"{mystr}\n    , {seq_str} int"
            clm_lst = tbl["columnList"]
            clm_str = ' text\n    , '.join([e["columnName"] for e in clm_lst])
            mystr = f"{mystr}\n    , {clm_str} text"
            mystr = f"{mystr}\n    );\n\n"
            str_buff = f"{str_buff}-- The {idx}-th table {tbl_name}\n{mystr}"
        with open(sql_file, 'w') as f:
            f.write(str_buff)

    def parse_to_csv(self):
        """
        *parse JSON data to csv format using map*

        * Based on data in **json_data** and map in **map**, parse JSON data
        * Store CSV format data into **parsed_tables**
        """
        psd_tbl = dict()
        for idx_tbl, csv_tbl in enumerate(self.map["tableList"], start=1):
            tblName = csv_tbl["tableName"]
            tblContent = []
            psd_tbl[tblName] = tblContent
        for js1 in self.json_data:
            js1[self.json_txn_id_name] = str(uuid.uuid4())
            tableCnt = self.map["tableNumber"]
            # logger.info(f"Work on {tableCnt} tables...")
            for idx_tbl, csv_tbl in enumerate(self.map["tableList"], start=1):
                tblName = csv_tbl["tableName"]
                tblContent = []
                tbl_content = list()
                seq_list = csv_tbl.get("seqList", None)
                if seq_list is None:
                    j_tbl = js1 # root table
                else:
                    j_tbl = compute_table_content(js1, seq_list, json_txn_id_name = self.json_txn_id_name)

                if seq_list is None: # root table
                    psd_str = parse(j_tbl, csv_tbl["columnList"], json_txn_id_name = self.json_txn_id_name, csv_delim = self.csv_delim)
                    if len(psd_str) > 0:
                        psd_tbl[tblName].append(psd_str)
                elif len(seq_list) == 1: # level 1 table
                    for elm in j_tbl:
                        psd_str = parse(elm, csv_tbl["columnList"], seq_list = seq_list, json_txn_id_name = self.json_txn_id_name, csv_delim = self.csv_delim)
                        if len(psd_str) > 0:
                            psd_tbl[tblName].append(psd_str)
                elif len(seq_list) == 2: # level 2 table
                    for elm_l in j_tbl:
                        for elm in elm_l:
                            psd_str = parse(elm, csv_tbl["columnList"], seq_list = seq_list, json_txn_id_name = self.json_txn_id_name, csv_delim = self.csv_delim)
                            if len(psd_str) > 0:
                                psd_tbl[tblName].append(psd_str)
                elif len(seq_list) == 3: # level 3 table
                    for elm_l in j_tbl:
                        for elm_2 in elm_l:
                            for elm in elm_2:
                                psd_str = parse(elm, csv_tbl["columnList"], seq_list = seq_list, json_txn_id_name = self.json_txn_id_name, csv_delim = self.csv_delim)
                                if len(psd_str) > 0:
                                    psd_tbl[tblName].append(psd_str)
                else:
                    logger.error("Do we need this level?")
                    exit()
        self.parsed_tables = psd_tbl

    def map_to_allpath(self):
        """ 
        *Compute all path from map*

        * This method is used when we like to check/update map using new JSON data
        * When we import map (from JSON or CSV format) into **map**
        * this method analyze **map** and store all path and array (table) into **map_path** and **map_array**
        * Later we can check new JSON data with **map_path** for new paths.
        """
        all_path = []
        all_array = []
        for tbl in self.map["tableList"]:
            if len(tbl["rootPath"]) > 0:
                all_array.append(tbl["rootPath"])
                for clm in tbl["columnList"]:
                    rootpath = tbl["rootPath"]
                    rel_path = clm["relativePath"]
                    thispath = f"{rootpath}.{rel_path}"
                    all_path.append(thispath)
            else:
                all_path += [clm["relativePath"] for clm in tbl["columnList"]]
        self.map_path = all_path
        self.map_array = all_array

    def add_new_path_to_map(self, new_path_list):
        """ 
        *Add new paths to map*

        * Assume no new table need to be added. If there exist new tables, will work on it in future.
        * based on *new_path_list* from the new JSON data and **map_path**
        * Add new path into **map**
        """
        for idx, new_path in enumerate(new_path_list, start=1):
            # logger.debug(f"Add {idx}-th new path '{new_path}'...")
            # if idx > 1:
            #     break
            tbl_path = None
            for arr in list(reversed(sorted(self.map_array))):
                # logger.debug(f"array check: {arr}")
                if arr in new_path:
                    tbl_path = arr
                    break
            # logger.debug(f"The new path '{new_path}' belong to array '{tbl_path}'")
            if tbl_path is None:
                logger.error("{new_path} DO NOT belong to any table!!!")
                exit()
            tbl_name = None
            for tbl in self.map["tableList"]:
                if tbl_path == tbl["rootPath"]:
                    tbl_name = tbl["tableName"]
                    clm_list = [clm["columnName"] for clm in tbl["columnList"]]
                    break
            if tbl_name is None:
                logger.error(f"Can NOT find table for {new_path} with array {tbl_path}!!!")
                exit()
            # logger.debug(f"The new path '{new_path}' belong to table '{tbl_name}'")
            # logger.debug(f"Existing columns: {clm_list}")
            clm = name_from_path(new_path, clm_list)
            rel_path = new_path[len(tbl_path)+1:]
            # logger.debug(f"Column name for '{new_path}' will be '{clm}' with relative path '{rel_path}'")
            j_clm = dict()
            j_clm["columnName"] = clm
            j_clm["relativePath"] = rel_path
            for tbl in self.map["tableList"]:
                if tbl_path == tbl["rootPath"]:
                    tbl["columnList"].append(j_clm)

def get_paths(source, flag_json_array):
    """ 
    *get full path*

    * This is out of CLASS **JsonUtils**
    * Code originally from https://stackoverflow.com/questions/51488240/python-get-json-keys-as-full-path
    * This is recursice function: call itself
    * called by **compute_all_paths**
    """
    paths = []
    if isinstance(source, collections.abc.MutableMapping):  # found a dict-like structure...
        for k, v in source.items():  # iterate over it; Python 2.x: source.iteritems()
            paths.append([k])  # add the current child path
            paths += [[k] + x for x in get_paths(v, flag_json_array)]  # get sub-paths, extend with the current
            #logger.info(f"DEBUG: mutablemapping {k} -> {paths}")
    # else, check if a list-like structure, remove if you don't want list paths included
    elif isinstance(source, collections.abc.Sequence) and not isinstance(source, str):
        #                          Python 2.x: use basestring instead of str ^
        for i, v in enumerate(source):
            # paths.append([i])
            # paths += [[i] + x for x in get_paths(v)]  # get sub-paths, extend with the current
            paths.append([flag_json_array])
            paths += [x for x in get_paths(v, flag_json_array)]  # get sub-paths, extend with the current
            #logger.info(f"DEBUG: sequence {i} -> {paths}")
    return paths

def name_from_path(path, name_list):
    """ 
    *Get variable name from path*

    * Giving *path* and existing *name_list*
    * compute name for this *path* (last element of the path)
    * If exist in *name_list*, create random one for later to rename
    * called by **table_plan_json** and **add_new_path_to_map**
    """
    elm = path.split('.')[-1]
    # while elm in name_list:
    while elm.upper() in [e.upper() for e in name_list]:
        rdmstr = str(uuid.uuid4())[:6]
        elm = f"{elm}_{rdmstr}"
    return elm

def parse(json_data, column_list, 
          seq_list = None, 
          debug_idx = 0, 
          json_txn_id_name = 'txn_uuid',
          csv_delim = ',',
         ):
    """ 
    *one level parse*

    * parse *json_data* based on *column_list*
    * Store the parsed result as text
    * Combine parsed text result using *csv_delim*
    * With *seq_list*, combine seq_no value to result
    * called by **parse_to_csv**
    """
    str_rst = ''
    clm_cnt = len(column_list)
    for clm_idx in range(clm_cnt):
        thispath = column_list[clm_idx]["relativePath"]
        thiscell = parse_tags_wo_arr(json_data, thispath)
        if thiscell is None:
            thiscell = ''

        if clm_idx == 0:
            str_rst = str(thiscell)
        else:
            str_rst = f"{str_rst}{csv_delim}{str(thiscell)}"
    if len(str_rst) < clm_cnt:
        return ""
    thiscell = json_data[json_txn_id_name]
    if seq_list is None:
        return f"{thiscell}{csv_delim}{str_rst}"
    str_pre = thiscell
    for d in seq_list:
        thispath = d["columnName"]
        thiscell = str(json_data[thispath])
        str_pre = f"{str_pre}{csv_delim}{thiscell}"
    return f"{str_pre}{csv_delim}{str_rst}"

def compute_table_content(json_data, seq_list, json_txn_id_name = 'txn_uuid'):
    """ 
    *compute table content based on seq_list (with array path)*

    * Based on *seq_list*, go to the level where data stored in.
    * for each level, keep seq_no to the lower level.
    * At the lowest level, combine all json element into js array.
    * return js array.
    * called by **parse_to_csv**
    """
    js = json_data
    for level_idx in range(len(seq_list)):
        if level_idx == 0:
            seq_path = seq_list[0]["arrayPath"]
            current_tags = seq_path

            temp_list = []
            js_arr = parse_tags_wo_arr(js, current_tags)
            for idx, e in enumerate(js_arr, start = 1):
                e.update({json_txn_id_name: js[json_txn_id_name]})
                e.update({seq_list[0]["columnName"]: idx})
                temp_list.append(e)
            js = temp_list
        elif level_idx == 1:
            seq_path = seq_list[1]["arrayPath"]
            current_tags = seq_path[len(seq_list[0]["arrayPath"])+1:]

            temp_list = []
            for e_1 in js: # now js is array already
                t1_arr = []
                js_arr = parse_tags_wo_arr(e_1, current_tags)
                for idx, e in enumerate(js_arr, start=1):
                    e.update({json_txn_id_name: e_1[json_txn_id_name]})
                    e.update({seq_list[0]["columnName"]: e_1[seq_list[0]["columnName"]]})
                    e.update({seq_list[1]["columnName"]: idx})
                    t1_arr.append(e)
                temp_list.append(t1_arr)
            js = temp_list
        elif level_idx == 2:
            seq_path = seq_list[2]["arrayPath"]
            current_tags = seq_path[len(seq_list[1]["arrayPath"])+1:]

            temp_list = []
            for e_1 in js: # now js is array already
                t1_arr = []
                for e_2 in e_1: # now e_2 is also array
                    t2_arr = []
                    js_arr = parse_tags_wo_arr(e_2, current_tags)
                    for idx, e in enumerate(js_arr, start=1):
                        e.update({json_txn_id_name: e_2[json_txn_id_name]})
                        e.update({seq_list[0]["columnName"]: e_2[seq_list[0]["columnName"]]})
                        e.update({seq_list[1]["columnName"]: e_2[seq_list[1]["columnName"]]})
                        e.update({seq_list[2]["columnName"]: idx})
                        t2_arr.append(e)
                    t1_arr.append(t2_arr)
                temp_list.append(t1_arr)
            js = temp_list
        elif level_idx == 3:
            seq_path = seq_list[3]["arrayPath"]
            current_tags = seq_path[len(seq_list[2]["arrayPath"])+1:]

            temp_list = []
            for e_1 in js: # now js is array already
                t1_arr = []
                for e_2 in e_1: # now e_2 is also array
                    t2_arr = []
                    for e_3 in e_2: # now e_3 is also array
                        t3_arr = []
                        js_arr = parse_tags_wo_arr(e_2, current_tags)
                        for idx, e in enumerate(js_arr, start=1):
                            e.update({json_txn_id_name: e_3[json_txn_id_name]})
                            e.update({seq_list[0]["columnName"]: e_3[seq_list[0]["columnName"]]})
                            e.update({seq_list[1]["columnName"]: e_3[seq_list[1]["columnName"]]})
                            e.update({seq_list[2]["columnName"]: e_3[seq_list[2]["columnName"]]})
                            e.update({seq_list[3]["columnName"]: idx})
                            t3_arr.append(e)
                        t2_arr.append(t1_arr)
                    t1_arr.append(t2_arr)
                temp_list.append(t1_arr)
            js = temp_list
        else:
            logger.error("Do we need this level?")
            exit()
    return js

def table_seq_list(path, arraylist):
    """ 
    *compute seqList based on table path in position of arraylist*

    * Given one *path*, compute which table it should be, and which seq_no it should have.
    * When path in *arraylist* (table) is subset of *path*, this *path* belong to this table. If this table have seq_no, this seq_no need to be used.
    * Set table name (last tag in table path) into column name
    * the seq_list will show which level of the table it is, like header table is level 0; item table is level 1; and itemdiscount table is level 2; etc.
    * if the table is level 0, no seq_list;
    * if the table is level 1, seq_list has one element;
    * if the table is level 2, seq_list have two elements.
    * called by **table_plan_json**
    """
    seq_list = []
    for tbl_p in reversed(arraylist):
        if tbl_p in path:
            tag = tbl_p.split('.')[-1]
            clm_nm = f"seq_{tag}"
            seq_list.append({"columnName": clm_nm, "arrayPath": tbl_p})
    rst = list(reversed(seq_list))
    return rst

def parse_tags_wo_arr(json_data, tags):
    """ 
    *parse multiple level of tag without array*

    In JSON format, the data can be stored in multiple levels.
    If all these levels are not array (list of values, which means, one transaction have multiple value for that tag),
    we can still treat it as transaction level variable (no need to store into separate table).

    * Given *json_data* and *tags*
    * for each level of *tags*, go inside of JSON element in *json_data*
    * If value does not exist in *tags* path, just return *None*
    * else, return the value according to *tags* path.
    * called by **parse** and **compute_table_content** (out of class JsonUtils)
    """
    pl = tags.split('.')
    thiscell = json_data
    for p in pl:
        if thiscell is None:
            break
        thiscell = thiscell.get(p, None)
    return thiscell


if __name__ == '__main__':
    crt_dir = os.getcwd()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # mylog = f"{crt_dir}/json_utils_{timestamp}.log"
    mylog = f"{crt_dir}/json_utils.log"
    if os.path.exists(mylog):
        os.remove(mylog)
    logzero.logfile(mylog)

    logger.info(f'start python code {__file__}.\n')
    logger.info(f'end python code {__file__}.\n')
