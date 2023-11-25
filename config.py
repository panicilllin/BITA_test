###
# config file for the project
###
import sqlalchemy as sa

input_conf = {
    "engine": "dask",  ## ["pandas", "dask"]
    "zip_path": "Stock.zip",
    "file_path": "Stock.CSV",
}

db_conf = {
    "engine": "Postgres",
    "Server": "localhost",
    "Database": "test",
    "Port": 5436,
    "Username": "postgres",
    "Password": ""
}

# below here is the config for other kinds of CSVs
# I don't want to write hard code, so I leave the config here
# if you are going to upload CSV in other format, add seperator and dtype info here

csv_conf = {
    "Stock.CSV": {
        "table_name": 'stock',
        "file_path": 'Stock.CSV',
        "batch": 1e6,
        "sep": ';',
        "dtype": {'PointOfSale': str,
                  'Product': str,
                  'Date': str,
                  'Stock': int
                  },
        "pg_type": {
            "PointOfSale": "CHAR(50)",
            "Product": "CHAR(50)",
            "Date": "DATE",
            "Stock": "INT"
        },
        "sa_dtype": {'PointOfSale': sa.types.String,
                     'Product': sa.types.String,
                     'Date': sa.types.String,
                     'Stock': sa.types.Integer}
    },
    "test_case.CSV": {
        "table_name": 'test_case',
        'file_path': 'test_case.CSV',
        "batch": 1e6,
        "sep": '; ',
        "dtype": {'PointOfSale': str,
                  'Product': str,
                  'Date': str,
                  'Stock': int
                  },
        "pg_type": {
                    "PointOfSale": "CHAR(50)",
                    "Product": "CHAR(50)",
                    "Date": "DATE",
                    "Stock": "INT"
                },
        "sa_dtype": {'PointOfSale': sa.types.String,
                     'Product': sa.types.String,
                     'Date': sa.types.String,
                     'Stock': sa.types.Integer}
    }
}
