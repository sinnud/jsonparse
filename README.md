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