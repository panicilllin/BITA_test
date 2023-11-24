###
# config file for the project
###

input_conf = {
    "engine": "Pandas",
    "zip_path": "Stock.zip",
    "file_path": "Stock.CSV",
    "batch": 1e6
}


output_conf = {
    "engine": "Postgres",
    "Server": "localhost",
    "Database": "postgres",
    "Port": 5436,
    "Username": "postgres",
    "Password": ""
}
