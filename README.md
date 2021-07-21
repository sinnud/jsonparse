## JSON utils
This module will: based on JSON data, create JSON format mapping file. Use JSON format mapping file to convert JSON data into CSV like data. Then we can load JSON data into database.

This module can:

- create JSON format map based on JSON data
- update map based on new JSON data
- parse data into CSV format

## compile
The command to compile the package is
```python -m build --wheel```
Assume you have `pip install build`

## install compiled package
```pip --disable-pip-version-check install --find-links ./dist jsonparse```

## Unittest for this development
The unittest can also be treated as sample of using this module.

- tests folder includes samples for test
- run test from the folder this file belong to with command `python -m unittest tests.[test_file].[test_func]`

## Auto doc generation
Create PDF version documentation, store as extra source file in repository.

- The python module sphinx to `make latex`
- The MikTeX software to `pdflatex jsonparse.tex`
- under linux (like ubuntu, with `latexmk` installed), just `make latexpdf`
- copy the PDF file to repository

## Updates

- `JsonUtils.get_json_len` and `JsonUtils.append_from_list` methods allow append JSON array to existing array.
- In test file, remove special symbols before go into JsonUtils.
- 18JUL2021 new version: using pool for computing all path and parsing data
  - The previous version have problem in parsing algorithm:
    - Code is very long with the following functions: `JsonUtils.parse_to_csv` calls `parse`, `compute_table_content`, `table_seq_list`, and `parse_tags_wo_arr`. The total code line number is 220.
    - Code is long because each array level is considered, but just considered to the third level. This means if some data have arran level 4 or more, this code will not work any more.
  - To improve this, the pool technique is used.
  - The pool technique is first used as alternative function of `get_paths`, which is recursive function.
  - The function `get_path_pool` will generate same result as funstion `get_paths`.
  - Looks the pool was not defined efficiently: with 12K records, the computational time from 5 seconds using `get_paths` increaded to 4 minutes using `get_path_pool`.
  - Based on this, as production code, we will still use `get_paths`. But the pool technique will be used for alternative of `JsonUtils.parse_to_csv`.
  - The alternative of `JsonUtils.parse_to_csv` method: `JsonUtils.parse_use_pool` has been developed:
    - The result is same to `JsonUtils.parse_to_csv`.
    - The code line number for `JsonUtils.parse_use_pool` calling `JsonUtils.gen_tblstr_by_map` as total is 80 (comparing with 220).
    - Since we didn't design it based on array level, it will work fine with any array level.
    - It also provides capability that, if data have new fields than map, collect them and remind developer (will be in soon).