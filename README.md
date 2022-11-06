# NILAM - BULK UPLOAD

For converting xlsx to csv, run the following commanda.

Install **in2csv** if you don't have it already.

`pip install in2csv`

The .xlsx files in xlsx folder will be converted to .csv files and stored in csv folder.

`python xlsx_to_csv.py`

**For Bulk upload**

Install the dependencies

`npm install`

Convert the Excel file to CSV

Run the command to upload data

`node nilam.js --file <file_path and name along with extension> --block <block_name-block_number>`

Example
`node nilam.js --file Kanakkampalayam.csv --block Kanakkampalayam-TNP-24`

You will be prompted for the Access token for the corresponding block AO. Enter the access token and hit enter.

The errors will be stored in a file in the current directory with the name `<block>-errors.csv`
